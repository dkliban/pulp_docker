from gettext import gettext as _
from urllib.parse import urljoin
import base64
import hashlib
import json
import logging

from pulpcore.plugin.models import Artifact, ProgressBar, Repository  # noqa
from pulpcore.plugin.stages import (
    DeclarativeArtifact,
    DeclarativeContent,
    DeclarativeVersion,
    Stage
)

# TODO(asmacdo) alphabetize
from pulp_docker.app.models import ImageManifest, DockerRemote, MEDIA_TYPE, ManifestBlob


log = logging.getLogger(__name__)


def synchronize(remote_pk, repository_pk):
    """
    Sync content from the remote repository.

    Create a new version of the repository that is synchronized with the remote.

    Args:
        remote_pk (str): The remote PK.
        repository_pk (str): The repository PK.

    Raises:
        ValueError: If the remote does not specify a URL to sync

    """
    remote = DockerRemote.objects.get(pk=remote_pk)
    repository = Repository.objects.get(pk=repository_pk)

    if not remote.url:
        raise ValueError(_('A remote must have a url specified to synchronize.'))

    first_stage = DockerFirstStage(remote)
    DeclarativeVersion(first_stage, repository).create()


class DockerFirstStage(Stage):
    """
    The first stage of a pulp_docker sync pipeline.
    """

    # TODO(asmacdo) self.remote is needed for Declarative artifacts, so needed by all plugins.
    # Add this to the base class?
    def __init__(self, remote):
        """
        The first stage of a pulp_docker sync pipeline.

        Args:
            remote (DockerRemote): The remote data to be used when syncing

        """
        self.remote = remote

    async def __call__(self, in_q, out_q):
        """
        Build and emit `DeclarativeContent` from the Manifest data.

        Args:
            in_q (asyncio.Queue): Unused because the first stage doesn't read from an input queue.
            out_q (asyncio.Queue): The out_q to send `DeclarativeContent` objects to

        """
        with ProgressBar(message="Downloading Tags") as pb:
            log.info("Fetching tags list for upstream repository: {repo}".format(
                repo=self.remote.upstream_name
            ))
            list_downloader = self.remote.get_downloader(self.tags_list_url)
            await list_downloader.run()

            with open(list_downloader.path) as tags_raw:
                tags_dict = json.loads(tags_raw.read())
                tag_list = tags_dict['tags']
            pb.increment()

        # TODO(asmacdo) Break this up
        with ProgressBar(message="Downloading Manifests by Tag"):
            skipped_content_types = set()
            # TODO(asmacdo) temporary, use all tags. need to add whitelist (sync.py#223)
            for tag in tag_list:
                # TODO(asmacdo)
                # tag_obj = Tag(tag)
                tag_url = self.get_tag_url(tag)
                log.info("Retriving tag from: {url}".format(url=tag_url))
                tag_downloader = self.remote.get_downloader(tag_url)
                # Accept headers indicate the highest version the client (us) can use.
                # The registry will return Manifests of this and lower type.
                # TODO(asmacdo) make this a constant?
                v2_accept_headers = {'Accept': ','.join([
                    MEDIA_TYPE.MANIFEST_V2,
                    MEDIA_TYPE.MANIFEST_LIST,
                ])}
                extra_request_data = {'headers': v2_accept_headers}
                await tag_downloader.run(extra_data=extra_request_data)
                data_type = tag_downloader.response_headers['Content-Type']
                # TODO(asmacdo) merge with other constants
                content_types = {
                    MEDIA_TYPE.MANIFEST_V2: 'TODO(asmacdo)self.process_manifest_v2',
                }
                try:
                    # TODO(asmacdo) remove or refactor
                    # type_builder = content_types[data_type]
                    content_types[data_type]
                except KeyError:
                    skipped_content_types.add(data_type)
                    continue

                with open(tag_downloader.path, 'rb') as manifest_file:
                    raw = manifest_file.read()
                manifest_digests = {k: v for k, v in tag_downloader.artifact_attributes.items()
                                    if k in Artifact.DIGEST_FIELDS}
                size = tag_downloader.artifact_attributes['size']
                manifest_data = json.loads(raw)

                manifest_artifact = Artifact(
                    # TODO(asmacdo) could size of an artifact be determined by the
                    # ArtifactFileField?
                    size=size,
                    # TODO(asmacdo) could the digest of an artifact be determined by the
                    # ArtifactFileField?
                    file=tag_downloader.path,
                    **manifest_digests
                )
                try:
                    manifest_artifact.save()
                except Exception as e:
                    sha256 = manifest_digests['sha256']
                    log.info("Artifact already exists, using {digest}".format(digest=sha256))
                    try:
                        manifest_artifact = Artifact.objects.get(sha256=sha256)
                    except Exception as e2:
                        import ipdb; ipdb.set_trace()
                        print(e2)

                da = DeclarativeArtifact(
                    artifact=manifest_artifact,
                    # TODO(asmacdo) should I set this to None or something?
                    url=tag_downloader.url,
                    # TODO(asmacdo) again, this is redundant
                    relative_path=tag_downloader.path,
                    remote=self.remote,
                    extra_data=extra_request_data,
                )
                manifest = ImageManifest(
                    digest=manifest_digests['sha256'],
                    schema_version=manifest_data['schemaVersion'],
                    media_type=manifest_data['mediaType']
                )
                try:
                    manifest.save()
                except Exception as e:
                    log.info("Manifest already created, using existing copy")
                    manifest = ImageManifest.objects.get(digest=manifest.digest)

                dc = DeclarativeContent(content=manifest, d_artifacts=[da])
                if dc.content.pk is None:
                    import ipdb; ipdb.set_trace()
                    print(dc.content)

                if da.artifact.pk is None:
                    import ipdb; ipdb.set_trace()
                    print(da.artifact)
                await out_q.put(dc)

                # TODO(asmacdo) is [] default mutable?
                for layer in manifest_data.get('layers', []):
                    sha256 = layer['digest'][len('sha256:'):]
                    blob_artifact = Artifact(
                        size=layer['size'],
                        sha256=sha256
                        # Size not set, its not downloaded yet
                    )
                    blob = ManifestBlob(
                        digest=sha256,
                        media_type=layer['mediaType'],
                        manifest=manifest
                    )
                    da = DeclarativeArtifact(
                        artifact=blob_artifact,
                        # Url should include 'sha256:'
                        url=self.layer_url(layer['digest']),
                        # TODO(asmacdo) is this what we want?
                        relative_path=layer['digest'],
                        remote=self.remote,
                        # extra_data="TODO(asmacdo)"
                    )
                    dc = DeclarativeContent(content=blob, d_artifacts=[da])
                    await out_q.put(dc)
        # Use ProgressBar to report progress
        # for entry in self.read_my_metadata_file_somehow(result.path):
        #     unit = ImageManifest(entry)  # make the content unit in memory-only
        #     artifact = Artifact(entry)  # make Artifact in memory-only
        #     da = DeclarativeArtifact(artifact, entry.url, entry.relative_path, self.remote)
        #     dc = DeclarativeContent(content=unit, d_artifacts=[da])
        #     await out_q.put(dc)
        # await out_q.put(None)
        log.warn("Skipped Content-Types: {types}".format(types=skipped_content_types))
        await out_q.put(None)

    def get_tag_url(self, tag):
        relative_url = '/v2/{name}/manifests/{tag}'.format(
            name=self.remote.namespaced_upstream_name,
            tag=tag
        )
        return urljoin(self.remote.url, relative_url)

    @property
    def tags_list_url(self):
        relative_url = '/v2/{name}/tags/list'.format(name=self.remote.namespaced_upstream_name)
        return urljoin(self.remote.url, relative_url)

    def layer_url(self, digest):
        relative_url = '/v2/{name}/blobs/{digest}'.format(
            name=self.remote.namespaced_upstream_name,
            digest=digest
        )
        return urljoin(self.remote.url, relative_url)


class UnitMixin(object):

    meta = {
        'abstract': True,
    }

    @staticmethod
    def calculate_digest(manifest, algorithm='sha256'):
        """
        Calculate the requested digest of the Manifest, given in JSON.

        :param manifest:  The raw JSON representation of the Manifest.
        :type  manifest:  basestring
        :param algorithm: The digest algorithm to use. Defaults to sha256. Must be one of the
                          algorithms included with hashlib.
        :type  algorithm: basestring
        :return:          The digest of the given Manifest
        :rtype:           basestring
        """
        decoded_manifest = json.loads(manifest, encoding='utf-8')
        if 'signatures' in decoded_manifest:
            # This manifest contains signatures. Unfortunately, the Docker manifest digest
            # is calculated on the unsigned version of the Manifest so we need to remove the
            # signatures. To do this, we will look at the 'protected' key within the first
            # signature. This key indexes a (malformed) base64 encoded JSON dictionary that
            # tells us how many bytes of the manifest we need to keep before the signature
            # appears in the original JSON and what the original ending to the manifest was after
            # the signature block. We will strip out the bytes after this cutoff point, add back the
            # original ending, and then calculate the sha256 sum of the transformed JSON to get the
            # digest.
            protected = decoded_manifest['signatures'][0]['protected']
            # Add back the missing padding to the protected block so that it is valid base64.
            protected = UnitMixin._pad_unpadded_b64(protected)
            # Now let's decode the base64 and load it as a dictionary so we can get the length
            protected = base64.b64decode(protected)
            protected = json.loads(protected)
            # This is the length of the signed portion of the Manifest, except for a trailing
            # newline and closing curly brace.
            signed_length = protected['formatLength']
            # The formatTail key indexes a base64 encoded string that represents the end of the
            # original Manifest before signatures. We will need to add this string back to the
            # trimmed Manifest to get the correct digest. We'll do this as a one liner since it is
            # a very similar process to what we've just done above to get the protected block
            # decoded.
            signed_tail = base64.b64decode(UnitMixin._pad_unpadded_b64(protected['formatTail']))
            # Now we can reconstruct the original Manifest that the digest should be based on.
            manifest = manifest[:signed_length] + signed_tail
        hasher = getattr(hashlib, algorithm)
        return "{a}:{d}".format(a=algorithm, d=hasher(manifest).hexdigest())

    @staticmethod
    def _pad_unpadded_b64(unpadded_b64):
        """
        Docker has not included the required padding at the end of the base64 encoded
        'protected' block, or in some encased base64 within it. This function adds the correct
        number of ='s signs to the unpadded base64 text so that it can be decoded with Python's
        base64 library.

        :param unpadded_b64: The unpadded base64 text
        :type  unpadded_b64: basestring
        :return:             The same base64 text with the appropriate number of ='s symbols
                             appended
        :rtype:              basestring
        """
        # The Pulp team has not observed any newlines or spaces within the base64 from Docker, but
        # Docker's own code does this same operation so it seemed prudent to include it here.
        # See lines 167 to 168 here:
        # https://github.com/docker/libtrust/blob/9cbd2a1374f46905c68a4eb3694a130610adc62a/util.go
        unpadded_b64 = unpadded_b64.replace('\n', '').replace(' ', '')
        # It is illegal base64 for the remainder to be 1 when the length of the block is
        # divided by 4.
        if len(unpadded_b64) % 4 == 1:
            raise ValueError(_('Invalid base64: {t}').format(t=unpadded_b64))
        # Add back the missing padding characters, based on the length of the encoded string
        paddings = {0: '', 2: '==', 3: '='}
        return unpadded_b64 + paddings[len(unpadded_b64) % 4]
