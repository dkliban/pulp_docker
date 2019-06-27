import hashlib
import re
from logging import getLogger
from types import SimpleNamespace

from django.db import models

from pulpcore.plugin.download import DownloaderFactory
from pulpcore.plugin.models import (Artifact, Content, Model, Remote, Repository,
                                    RepositoryVersion, RepositoryVersionDistribution)

from . import downloaders


logger = getLogger(__name__)


MEDIA_TYPE = SimpleNamespace(
    MANIFEST_V1='application/vnd.docker.distribution.manifest.v1+json',
    MANIFEST_V1_SIGNED='application/vnd.docker.distribution.manifest.v1+prettyjws',
    MANIFEST_V2='application/vnd.docker.distribution.manifest.v2+json',
    MANIFEST_LIST='application/vnd.docker.distribution.manifest.list.v2+json',
    CONFIG_BLOB='application/vnd.docker.container.image.v1+json',
    REGULAR_BLOB='application/vnd.docker.image.rootfs.diff.tar.gzip',
    FOREIGN_BLOB='application/vnd.docker.image.rootfs.foreign.diff.tar.gzip',
)


class ManifestBlob(Content):
    """
    A blob defined within a manifest.

    The actual blob file is stored as an artifact.

    Fields:
        digest (models.CharField): The blob digest.
        media_type (models.CharField): The blob media type.

    Relations:
        manifest (models.ForeignKey): Many-to-one relationship with Manifest.
    """

    TYPE = 'manifest-blob'

    digest = models.CharField(max_length=255)
    media_type = models.CharField(
        max_length=80,
        choices=(
            (MEDIA_TYPE.CONFIG_BLOB, MEDIA_TYPE.CONFIG_BLOB),
            (MEDIA_TYPE.REGULAR_BLOB, MEDIA_TYPE.REGULAR_BLOB),
            (MEDIA_TYPE.FOREIGN_BLOB, MEDIA_TYPE.FOREIGN_BLOB),
        ))

    class Meta:
        unique_together = ('digest',)


class Manifest(Content):
    """
    A docker manifest.

    This content has one artifact.

    Fields:
        digest (models.CharField): The manifest digest.
        schema_version (models.IntegerField): The docker schema version.
        media_type (models.CharField): The manifest media type.

    Relations:
        blobs (models.ManyToManyField): Many-to-many relationship with ManifestBlob.
        config_blob (models.ForeignKey): Blob that contains configuration for this Manifest.
        listed_manifests (models.ManyToManyField): Many-to-many relationship with Manifest. This
            field is used only for a manifest-list type Manifests.
    """

    TYPE = 'manifest'

    digest = models.CharField(max_length=255)
    schema_version = models.IntegerField()
    media_type = models.CharField(
        max_length=60,
        choices=(
            (MEDIA_TYPE.MANIFEST_V1, MEDIA_TYPE.MANIFEST_V1),
            (MEDIA_TYPE.MANIFEST_V2, MEDIA_TYPE.MANIFEST_V2),
            (MEDIA_TYPE.MANIFEST_LIST, MEDIA_TYPE.MANIFEST_LIST),
        ))

    blobs = models.ManyToManyField(ManifestBlob, through='BlobManifestBlob')
    config_blob = models.ForeignKey(ManifestBlob, related_name='config_blob',
                                    null=True, on_delete=models.CASCADE)

    # Order matters for through fields, (source, target)
    listed_manifests = models.ManyToManyField(
        "self",
        through='ManifestListManifest',
        symmetrical=False,
        through_fields=('image_manifest', 'manifest_list')
    )

    class Meta:
        unique_together = ('digest',)


class BlobManifestBlob(models.Model):
    """
    Many-to-many relationship between ManifestBlobs and ImageManifests.
    """

    manifest = models.ForeignKey(
        Manifest, related_name='blob_manifests', on_delete=models.CASCADE)
    manifest_blob = models.ForeignKey(
        ManifestBlob, related_name='manifest_blobs', on_delete=models.CASCADE)

    class Meta:
        unique_together = ('manifest', 'manifest_blob')


class ManifestListManifest(models.Model):
    """
    The manifest referenced by a manifest list.

    Fields:
        architecture (models.CharField): The platform architecture.
        variant (models.CharField): The platform variant.
        features (models.TextField): The platform features.
        os (models.CharField): The platform OS name.
        os_version (models.CharField): The platform OS version.
        os_features (models.TextField): The platform OS features.

    Relations:
        manifest (models.ForeignKey): Many-to-one relationship with Manifest.
        manifest_list (models.ForeignKey): Many-to-one relationship with ManifestList.
    """

    architecture = models.CharField(max_length=255)
    os = models.CharField(max_length=255)
    os_version = models.CharField(max_length=255)
    os_features = models.TextField(default='', blank=True)
    features = models.TextField(default='', blank=True)
    variant = models.CharField(max_length=255)

    image_manifest = models.ForeignKey(
        Manifest, related_name='image_manifests', on_delete=models.CASCADE)
    manifest_list = models.ForeignKey(
        Manifest, related_name='manifest_lists', on_delete=models.CASCADE)

    class Meta:
        unique_together = ('image_manifest', 'manifest_list')


class ManifestTag(Content):
    """
    A tagged Manifest.

    Fields:
        name (models.CharField): The tag name.

    Relations:
        tagged_manifest (models.ForeignKey): A referenced Manifest.

    """

    TYPE = 'manifest-tag'

    name = models.CharField(max_length=255, db_index=True)

    tagged_manifest = models.ForeignKey(
        Manifest, null=True, related_name='tagged_manifests', on_delete=models.CASCADE)

    class Meta:
        unique_together = (
            ('name', 'tagged_manifest'),
        )


class DockerRemote(Remote):
    """
    A Remote for DockerContent.

    Fields:
        upstream_name (models.CharField): The name of the image at the remote.
        include_foreign_layers (models.BooleanField): Foreign layers in the remote
            are included. They are not included by default.
    """

    upstream_name = models.CharField(max_length=255, db_index=True)
    include_foreign_layers = models.BooleanField(default=False)
    whitelist_tags = models.TextField(null=True)

    TYPE = 'docker'

    @property
    def download_factory(self):
        """
        Return the DownloaderFactory which can be used to generate asyncio capable downloaders.

        Upon first access, the DownloaderFactory is instantiated and saved internally.

        Plugin writers are expected to override when additional configuration of the
        DownloaderFactory is needed.

        Returns:
            DownloadFactory: The instantiated DownloaderFactory to be used by
                get_downloader()

        """
        try:
            return self._download_factory
        except AttributeError:
            self._download_factory = DownloaderFactory(
                self,
                downloader_overrides={
                    'http': downloaders.RegistryAuthHttpDownloader,
                    'https': downloaders.RegistryAuthHttpDownloader,
                }
            )
            return self._download_factory

    def get_downloader(self, remote_artifact=None, url=None, **kwargs):
        """
        Get a downloader from either a RemoteArtifact or URL that is configured with this Remote.

        This method accepts either `remote_artifact` or `url` but not both. At least one is
        required. If neither or both are passed a ValueError is raised.

        Args:
            remote_artifact (:class:`~pulpcore.app.models.RemoteArtifact`): The RemoteArtifact to
                download.
            url (str): The URL to download.
            kwargs (dict): This accepts the parameters of
                :class:`~pulpcore.plugin.download.BaseDownloader`.

        Raises:
            ValueError: If neither remote_artifact and url are passed, or if both are passed.

        Returns:
            subclass of :class:`~pulpcore.plugin.download.BaseDownloader`: A downloader that
            is configured with the remote settings.

        """
        kwargs['remote'] = self
        return super().get_downloader(remote_artifact=remote_artifact, url=url, **kwargs)

    @property
    def namespaced_upstream_name(self):
        """
        Returns an upstream Docker repository name with a namespace.

        For upstream repositories that do not have a namespace, the convention is to use 'library'
        as the namespace.
        """
        # Docker's registry aligns non-namespaced images to the library namespace.
        docker_registry = re.search(r'registry[-,\w]*.docker.io', self.url, re.IGNORECASE)
        if '/' not in self.upstream_name and docker_registry:
            return 'library/{name}'.format(name=self.upstream_name)
        else:
            return self.upstream_name


class DockerDistribution(RepositoryVersionDistribution):
    """
    A docker distribution defines how a publication is distributed by Pulp's webserver.
    """

    TYPE = 'docker'

    def get_repository_version(self):
        """
        Returns the repository version that is supposed to be served by this DockerDistribution.
        """
        if self.repository:
            return RepositoryVersion.latest(self.repository)
        elif self.repository_version:
            return self.repository_version
        else:
            return None

import os
import time

INCOMPLETE_EXT = '.part'

def generate_filename(instance, filename):
    filename = os.path.join(instance.upload_dir, str(instance.pk) + INCOMPLETE_EXT)
    return time.strftime(filename)

class Upload(Model):
    """
    Model for tracking Blob uploads.
    """
    repository = models.ForeignKey(
        Repository, related_name='uploads', on_delete=models.CASCADE)

    offset = models.BigIntegerField(default=0)

    file = models.FileField(max_length=255, null=True, upload_to=generate_filename)

    upload_dir = '/var/lib/pulp/tmp'

    size = models.IntegerField(null=True)
    md5 = models.CharField(max_length=32, null=True)
    sha1 = models.CharField(max_length=40, null=True)
    sha224 = models.CharField(max_length=56, null=True)
    sha256 = models.CharField(max_length=64, null=True)
    sha384 = models.CharField(max_length=96, null=True)
    sha512 = models.CharField(max_length=128, null=True)

    def append_chunk(self, chunk, chunk_size=None, save=True):
        hashers = {}
        for algorithm in Artifact.DIGEST_FIELDS:
            hashers[algorithm] = getattr(hashlib, algorithm)()

        self.file.close()
        self.file.open(mode='ab')  # mode = append+binary
        while True:
            subchunk = chunk.read(2000000)
            if not subchunk:
                break
            self.file.write(subchunk)
            for algorithm in Artifact.DIGEST_FIELDS:
                hashers[algorithm].update(subchunk)

        if chunk_size is not None:
            self.offset += chunk_size
        elif hasattr(chunk, 'size'):
            self.offset += chunk.size
        else:
            self.offset = self.file.size
        if save:
            self.save()
        self.file.close() # Flush
        for algorithm in Artifact.DIGEST_FIELDS:
            setattr(self, algorithm, hashers[algorithm].hexdigest())

