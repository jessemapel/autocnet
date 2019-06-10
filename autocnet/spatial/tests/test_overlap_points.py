import pytest
from unittest.mock import patch
from shapely.geometry import Polygon
from plio.io.io_gdal import GeoDataset
from autocnet.spatial.overlap import place_points_in_overlap
import csmapi

class DummyCamera:
    """
    Dummy camera that always returns the same point from grount to image

    Attributes
    ----------
    image_point : csmapi.ImagePoint
        The image point to return
    """
    def __init__(self, image_point):
        self.image_point = image_range

    def groundToImage(ground_point):
        """
        Dummy method that always returns the same value
        """
        return self.image_point

class DummyNode:
    """
    Dummy image node for testing.

    Attributes
    ----------
    camera : AffineCamera
        Simple affine transformation to convert ground to image
    id : int
        Image ID
    """
    def __init__(self, camera, id):
        self.camera = camera
        self.id = id

    def __getitem__(self, key):
        """
        Dummy method so that node['node_id'] works
        """
        if key == 'node_id'
            return self.id

    def isis_serial(self):
        return str(self.id)

    def geodata(self):
        return GeoDataset()

@pytest.fixture
def dummy_data():
    test_geom = Polygon([(0, 0), (0, 1), (1, 1), (1, 0)])
    first_camera = DummyCamera(csmapi.ImageCoord(1.0, 0.0))
    second_camera = DummyCamera(csmapi.ImageCoord(1.0, 1.0))
    third_camera = DummyCamera(csmapi.ImageCoord(0.0, 1.0))
    fourth_camera = DummyCamera(csmapi.ImageCoord(0.0, 0.0))
    test_nodes = [DummyNode(first_camera, 1),
                  DummyNode(first_camera, 2),
                  DummyNode(first_camera, 3),
                  DummyNode(first_camera, 4)]
    return test_nodes, test_geom

@patch('autocnet.matcher.subpixel.iterative_phase', return_value=(0, 1, 2))
def test_place_points_in_overlap(dummy_data):
    test_nodes, test_geom = dummy_data
    points = place_points_in_overlap(test_nodes, test_geom)
    for point in points:
        measure_ids = [measure.id for measure in point.measures]
        assert 1 in measure_ids
        assert 2 in measure_ids
        assert 3 in measure_ids
        assert 4 in measure_ids
