from django.conf.urls import url, include
from rest_framework.routers import Route, SimpleRouter
from pulp_docker.app.viewsets import (
    Blobs,
    BlobUploads,
    Manifests,
    VersionView
)

router = SimpleRouter(trailing_slash=False)

head_route = Route(
            url=r'^{prefix}/{lookup}{trailing_slash}$',
            mapping={
                'head': 'head',
            },
            name='{basename}-detail',
            detail=True,
            initkwargs={'suffix': 'Instance'}
        )

router.routes.append(head_route)
router.register(r'^v2/(?P<path>.+)/blobs/uploads\/?', BlobUploads, basename='docker-upload')
router.register(r'^v2/(?P<path>.+)/blobs', Blobs, basename='blobs')
router.register(r'^v2/(?P<path>.+)/manifests', Manifests, basename='manifests')

urlpatterns = [
    url(r'^v2/$', VersionView.as_view()),
    url(r'', include(router.urls))
]
