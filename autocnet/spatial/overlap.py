import warnings
from autocnet import config, Session, engine
from autocnet.cg import cg as compgeom
from autocnet.io.db.model import Images, Measures, Overlay, Points
from autocnet.matcher.subpixel import iterative_phase

import json
from redis import StrictRedis
from plurmy import Slurm
import csmapi
import pyproj
import shapely
import sqlalchemy


def place_points_in_overlaps(cg, size_threshold=0.0007, reference=None, height=0,
                             iterative_phase_kwargs={'size':71}):
    """
    Place points in all of the overlap geometries by back-projecing using
    sensor models.

    Parameters
    ----------
    cg : CandiateGraph object
         that is used to access sensor information

    size_threshold : float
                     overlaps with area <= this threshold are ignored

    reference : int
                the i.d. of a reference node to use when placing points. If not
                speficied, this is the node with the lowest id

    height : numeric
             The distance (in meters) above or below the aeroid (meters above or
             below the BCBF spheroid).
    """
    if not Session:
        warnings.warn('This function requires a database connection configured via an autocnet config file.')
        return

    points = []
    session = Session()
    srid = config['spatial']['srid']
    semi_major = config['spatial']['semimajor_rad']
    semi_minor = config['spatial']['semiminor_rad']
    ecef = pyproj.Proj(proj='geocent', a=semi_major, b=semi_minor)
    lla = pyproj.Proj(proj='latlon', a=semi_major, b=semi_minor)

    # TODO: This should be a passable query where we can subset.
    for o in session.query(Overlay).\
             filter(sqlalchemy.func.ST_Area(Overlay.geom) >= size_threshold):

        valid = compgeom.distribute_points_in_geom(o.geom)
        if not valid:
            continue

        overlaps = o.intersections

        if overlaps == None:
            continue

        if reference is None:
            source = overlaps[0]
        else:
            source = reference
        overlaps.remove(source)
        source = cg.node[source]['data']
        source_camera = source.camera
        for v in valid:
            point = Points(geom=shapely.geometry.Point(*v),
                           pointtype=2) # Would be 3 or 4 for ground

            # Get the BCEF coordinate from the lon, lat
            x, y, z = pyproj.transform(lla, ecef, v[0], v[1], height)  # -3000 working well in elysium, need aeroid
            gnd = csmapi.EcefCoord(x, y, z)

            # Grab the source image. This is just the node with the lowest ID, nothing smart.
            sic = source_camera.groundToImage(gnd)
            point.measures.append(Measures(sample=sic.samp,
                                           line=sic.line,
                                           imageid=source['node_id'],
                                           serial=source.isis_serial,
                                           measuretype=3))


            for i, d in enumerate(overlaps):
                destination = cg.node[d]['data']
                destination_camera = destination.camera
                dic = destination_camera.groundToImage(gnd)
                dx, dy, metrics = iterative_phase(sic.samp, sic.line, dic.samp, dic.line,
                                                  source.geodata, destination.geodata,
                                                  **iterative_phase_kwargs)
                if dx is not None or dy is not None:
                    point.measures.append(Measures(sample=dx,
                                                   line=dy,
                                                   imageid=destination['node_id'],
                                                   serial=destination.isis_serial,
                                                   measuretype=3))
            if len(point.measures) >= 2:
                points.append(point)
    session.add_all(points)
    session.commit()

def cluster_place_points_in_overlaps(size_threshold=0.0007, height=0,
                                     iterative_phase_kwargs={'size':71},
                                     walltime='00:10:00'):
    """
    Place points in all of the overlap geometries by back-projecing using
    sensor models. This method uses the cluster to process all of the overlaps
    in parallel. See place_points_in_overlap.

    Parameters
    ----------
    size_threshold : float
        overlaps with area <= this threshold are ignored

    height : numeric
         The distance (in meters) above or below the BCBF spheroid

    iterative_phase_kwargs : dict
        Dictionary of keyword arguments for the iterative phase matcher function

    walltime : str
        Cluster job wall time as a string HH:MM:SS
    """
    # Get all of the overlaps over the size threshold
    session = Session()
    overlaps = session.query(Overlay.id, Overlay.geom, Overlay.intersections).\
                       filter(sqlalchemy.func.ST_Area(Overlay.geom) >= size_threshold).\
                       filter(sqlalchemy.func.array_length(Overlay.intersections, 1) > 1)

    # Setup the redis queue
    rqueue = StrictRedis(host=config['redis']['host'],
                         port=config['redis']['port'],
                         db=0)

    # Push the job messages onto the queue
    queuename = config['redis']['processing_queue']
    for overlap in overlaps:
        msg = {'id' : overlap.id,
               'height' : height,
               'iterative_phase_kwargs' : iterative_phase_kwargs,
               'walltime' : walltime}
        rqueue.rpush(queuename, json.dumps(msg))
    job_counter = len([*overlaps]) + 1

    # Submit the jobs
    submitter = Slurm('acn_overlaps',
                 mem_per_cpu=config['cluster']['processing_memory'],
                 time=walltime,
                 partition=config['cluster']['queue'],
                 output=config['cluster']['cluster_log_dir']+'/slurm-%A_%a.out')
    submitter.submit(array='1-{}'.format(job_counter))
    return job_counter

def place_points_in_overlap(nodes, geom, height=0,
                            iterative_phase_kwargs={'size':71}):
    """
    Place points into an overlap geometry by back-projecing using sensor models.

    Parameters
    ----------
    nodes : list of Nodes
        The CandidateGraph nodes of all the images that intersect the overlap

    geom : geometry
        The geometry of the overlap region

    height : numeric
         The distance (in meters) above or below the BCBF spheroid

    iterative_phase_kwargs : dict
        Dictionary of keyword arguments for the iterative phase matcher function

    Returns
    -------
    points : list of Points
        The list of points seeded in the overlap
    """
    points = []
    semi_major = config['spatial']['semimajor_rad']
    semi_minor = config['spatial']['semiminor_rad']
    ecef = pyproj.Proj(proj='geocent', a=semi_major, b=semi_minor)
    lla = pyproj.Proj(proj='latlon', a=semi_major, b=semi_minor)

    valid = compgeom.distribute_points_in_geom(geom)
    if not valid:
        raise ValueError('Failed to distribute points in overlap')

    # Grab the source image. This is just the node with the lowest ID, nothing smart.
    source = nodes[0]
    nodes.remove(source)
    source_camera = source.camera
    for v in valid:
        geom = shapely.geometry.Point(v[0], v[1])
        point = Points(geom=geom,
                       pointtype=2) # Would be 3 or 4 for ground

        # Get the BCEF coordinate from the lon, lat
        x, y, z = pyproj.transform(lla, ecef, v[0], v[1], height)  # -3000 working well in elysium, need aeroid
        gnd = csmapi.EcefCoord(x, y, z)

        sic = source_camera.groundToImage(gnd)
        point.measures.append(Measures(sample=sic.samp,
                                       line=sic.line,
                                       imageid=source['node_id'],
                                       serial=source.isis_serial,
                                       measuretype=3))


        for i, dest in enumerate(nodes):
            dic = dest.camera.groundToImage(gnd)
            dx, dy, metrics = iterative_phase(sic.samp, sic.line, dic.samp, dic.line,
                                              source.geodata, dest.geodata,
                                              **iterative_phase_kwargs)
            if dx is not None or dy is not None:
                point.measures.append(Measures(sample=dx,
                                               line=dy,
                                               imageid=dest['node_id'],
                                               serial=dest.isis_serial,
                                               measuretype=3))
        if len(point.measures) >= 2:
            points.append(point)
    return points
