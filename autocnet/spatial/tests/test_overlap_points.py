import pytest
from unittest.mock import patch
from unittest.mock import MagicMock
from shapely.geometry import Polygon
from plio.io.io_gdal import GeoDataset
from autocnet.spatial.overlap import place_points_in_overlap
from autocnet.graph.node import Node
import csmapi

@pytest.fixture
def dummy_images():
    first_node = MagicMock(spec=Node)
    first_node.camera = MagicMock(spec=csmapi.RasterGm)
    first_node.camera.groundToImage.return_value = csmapi.ImageCoord(1.0, 0.0)
    first_node.isis_serial = '1'
    first_node.__getitem__ = 1
    first_node.geodata = GeoDataset()
    second_node = MagicMock(spec=Node)
    second_node.camera = MagicMock(spec=csmapi.RasterGm)
    second_node.camera.groundToImage.return_value = csmapi.ImageCoord(1.0, 1.0)
    second_node.isis_serial = '2'
    second_node.__getitem__ = 2
    second_node.geodata = GeoDataset()
    third_node = MagicMock(spec=Node)
    third_node.camera = MagicMock(spec=csmapi.RasterGm)
    third_node.camera.groundToImage.return_value = csmapi.ImageCoord(0.0, 1.0)
    third_node.isis_serial = '3'
    third_node.__getitem__ = 3
    third_node.geodata = GeoDataset()
    fourth_node = MagicMock(spec=Node)
    fourth_node.camera = MagicMock(spec=csmapi.RasterGm)
    fourth_node.camera.groundToImage.return_value = csmapi.ImageCoord(0.0, 0.0)
    fourth_node.isis_serial = '4'
    fourth_node.__getitem__ = 4
    fourth_node.geodata = GeoDataset()
    return [first_node, second_node, third_node, fourth_node]

@pytest.fixture
def dummy_geom():
    return Polygon([(0, 0), (0, 10), (10, 10), (10, 0)])

@patch('autocnet.matcher.subpixel.iterative_phase', return_value=(0, 1, 2))
def test_place_points_in_overlap(dummy_images, dummy_geom):
    test_nodes = dummy_images
    test_geom = dummy_geom
    points = place_points_in_overlap(test_nodes, test_geom)
    for point in points:
        measure_ids = [measure.id for measure in point.measures]
        assert sorted(measure_ids) == [1, 2, 3, 4]
