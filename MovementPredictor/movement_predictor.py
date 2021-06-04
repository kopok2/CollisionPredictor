"""
Movement and Collision Prediction System.

2021 by Karol Oleszek

Main analytical module for object detection, tracking, motion and collions prediction.
"""

import math
import json
import numpy as np
import pyqtgraph as pg
from typing import Optional
from operator import itemgetter
from kafka import KafkaConsumer
from PyQt5 import QtGui, QtCore
from pyqtrect import RectItem
from sklearn.cluster import MeanShift

# Object history tracking
snapshots = []
frames = []
previous_frames = []

# Visualization configuration
app = QtGui.QApplication([])
pw = pg.plot()
timer = pg.QtCore.QTimer()

# Kafka configuration
consumer = KafkaConsumer('point-plain',
                         group_id='movement-predictor',
                         bootstrap_servers=['localhost:9092'],
                         auto_commit_interval_ms=100,
                         enable_auto_commit=True,
                         auto_offset_reset='latest')


def update(points_vector: list,
           boxes: list,
           history: list,
           future: list,
           collisions: list) -> None:
    """
    Update motion and collision prediction visualization.

    :param points_vector: All points detected by LIDAR in the current frame.
    :param boxes: Bounding boxes for detected objects.
    :param history: Objects movement historical paths marks.
    :param future: Objects predicted future positions.
    :param collisions: Objects predicted collisions regions.
    """
    # Plot points
    x = np.array([x[0] for x in points_vector])
    y = np.array([x[1] for x in points_vector])
    pw.plot(x, y, clear=True, pen=None, symbol="x")

    # Plot boxes
    for box in history:
        rect_item = RectItem(QtCore.QRectF(*box), 'y')
        pw.addItem(rect_item)
    for box in boxes:
        rect_item = RectItem(QtCore.QRectF(*box), 'g')
        pw.addItem(rect_item)
    for box in future:
        rect_item = RectItem(QtCore.QRectF(*box), 'b')
        pw.addItem(rect_item)
    for box in collisions:
        rect_item = RectItem(QtCore.QRectF(*box), 'r', fill='r')
        pw.addItem(rect_item)


def detect_objects(plain_points: list) -> dict:
    """
    Object detection function.

    Fit clustering model, extract clusters bounding boxes and centers.

    :param plain_points: 2D points on a plane.
    :return: Detected objects bounding boxes and centers.
    """
    # Fit clustering model
    points = np.array(plain_points)
    bandwidth = 2
    ms = MeanShift(bandwidth=bandwidth, bin_seeding=True)
    ms.fit(points)

    # Extract clusters
    labels = ms.labels_
    labels_unique = np.unique(labels)
    n_clusters_ = len(labels_unique)

    # Prepare results
    result = {
        'boxes': [],
        'centers': []
    }
    box_margin = 0.5

    # Construct bounding boxes and centers for each cluster
    for k in range(n_clusters_):
        class_members = labels == k
        min_x, max_x, min_y, max_y = math.inf, -math.inf, math.inf, -math.inf
        for x in points[class_members]:
            min_x = min(min_x, x[0])
            min_y = min(min_y, x[1])
            max_x = max(max_x, x[0])
            max_y = max(max_y, x[1])
        result['boxes'].append((min_x-box_margin,
                                min_y-box_margin,
                                max_x - min_x + 2 * box_margin,
                                max_y - min_y + 2 * box_margin))
        result['centers'].append(((max_x + min_x) / 2,
                                  (max_y + min_y) / 2))
    print(result)
    print(n_clusters_)
    return result


def len_similarity_score(len1: float,
                         len2: float) -> float:
    """
    Calculate how similar are two lengths.

    :param len1: First length.
    :param len2: Second length.
    :return: Length relative similarity score.
    """
    scale = len1 / len2
    if scale <= 1:
        return scale
    else:
        return 1 / scale


def box_similarity_score(box1: list,
                         box2: list) -> float:
    """
    Calculate how similar are two bounding boxes.

    :param box1: First box.
    :param box2: Second box.
    :return: Box similarity score.
    """
    return 0.5 * len_similarity_score(box1[2], box2[2]) + 0.5 * len_similarity_score(box1[3], box2[3])


def distance_score(pos1: list,
                   pos2: list,
                   scene_x_scale: float,
                   scene_y_scale: float):
    """
    Calculate distance score between two points using scene relative scale.

    :param pos1: First point.
    :param pos2: Second point.
    :param scene_x_scale: X scene scale.
    :param scene_y_scale: Y scene scale.
    :return: Distance score.
    """
    # Calculate distances
    dif_x = abs(pos1[0] - pos2[0])
    dif_y = abs(pos1[1] - pos2[1])

    # Calculate relative distances
    scale_x = dif_x / scene_x_scale
    scale_y = dif_y / scene_y_scale

    return 1 - (0.5 * scale_x + 0.5 * scale_y)


def object_similarity_score(obj1: dict,
                            obj2: dict,
                            scene_x_scale: float,
                            scene_y_scale: float) -> float:
    """
    Calculate how similar are two detected objects.

    :param obj1: First object.
    :param obj2: Second object.
    :param scene_x_scale: X scene scale.
    :param scene_y_scale: Y scene scale.
    :return: Object similarity score.
    """
    return 0.5 * box_similarity_score(obj1['box'],
                                      obj2['box']) + 0.5 * distance_score(obj1['center'],
                                                                          obj2['center'],
                                                                          scene_x_scale,
                                                                          scene_y_scale)


def detect_object_pairs(new_objects: dict,
                        old_objects: dict) -> list:
    """
    Detect object pairs between scene observations.

    :param new_objects: Newly detected objects.
    :param old_objects: Objects detected in the last scene.
    :return: List of object pairs with matching identities.
    """
    # Detect scene total scale
    min_x, max_x, min_y, max_y = math.inf, -math.inf, math.inf, -math.inf
    for x in new_objects['centers']:
        min_x = min(min_x, x[0])
        min_y = min(min_y, x[1])
        max_x = max(max_x, x[0])
        max_y = max(max_y, x[1])
    scene_x_scale = max_x - min_x
    scene_y_scale = max_y - min_y
    print(scene_x_scale, scene_y_scale)

    # Prepare processing
    pairs = []
    used_old = set()
    used_new = set()
    result = []

    # For each new-old pair:
    for i in range(len(new_objects['centers'])):
        for j in range(len(old_objects['centers'])):
            # Calculate similarity
            first_object = {'box': new_objects['boxes'][i], 'center': new_objects['centers'][i]}
            second_object = {'box': old_objects['boxes'][j], 'center': old_objects['centers'][j]}
            similarity = object_similarity_score(first_object,
                                                 second_object,
                                                 scene_x_scale,
                                                 scene_y_scale)

            # Use threshold to detect identity
            if similarity > 0.5:
                pairs.append((i, j, similarity, first_object, second_object))

    # Sort pairs by similarity descending
    pairs.sort(key=itemgetter(2), reverse=True)

    # Recursively detect object pairs with matching identity
    for pair in pairs:
        if pair[0] not in used_new and pair[1] not in used_old:
            result.append(pair)
            used_new.add(pair[0])
            used_old.add(pair[1])

    print(result)
    return result


def get_object_movements(object_pairs: list) -> list:
    """
    Calculate objects motion speed.

    :param object_pairs: Detected matched objects.
    :return: List of objects with detected speed.
    """
    movements = []
    for pair in object_pairs:
        # Calculate object speed
        x_dif = pair[4]['center'][0] - pair[3]['center'][0]
        y_dif = pair[4]['center'][1] - pair[3]['center'][1]
        if x_dif or y_dif:
            movements.append({'pair': pair, 'vx': x_dif, 'vy': y_dif})
    movements.sort(key=lambda v: v['vx'] + v['vx'], reverse=True)
    print(movements)
    print(f"Detected {len(movements)} moving objects.")
    return movements


def shift_box(box: list,
              x: float,
              y: float) -> tuple:
    """
    Shift original box by coordinates.

    :param box: Original box.
    :param x: X shift value.
    :param y: Y shift value.
    :return: Shifted box
    """
    return box[0] - x, box[1] - y, box[2], box[3]


def predict_object_positions(object_movements: list,
                             moves_history: dict,
                             objects: dict) -> tuple:
    """
    Predict future objects positions.

    :param object_movements:
    :param moves_history:
    :param objects:
    :return:
    """
    # Get scene scale
    min_x, max_x, min_y, max_y = math.inf, -math.inf, math.inf, -math.inf
    for x in objects['centers']:
        min_x = min(min_x, x[0])
        min_y = min(min_y, x[1])
        max_x = max(max_x, x[0])
        max_y = max(max_y, x[1])
    print(moves_history)

    # Configure prediction
    result = []
    moving = []
    t_plus = 12
    for movement in object_movements:
        key = str(movement['pair'][4]['center'])
        vx = movement['vx']
        vy = movement['vy']

        # Check for acceleration
        if key in moves_history:
            # Calculate acceleration
            ax = vx - moves_history[key][0]
            ay = vy - moves_history[key][1]
            change_x = abs(ax) / abs(vx + 0.00000001)
            change_y = abs(ay) / abs(vy + 0.00000001)
            print("Tracking continuous movement:")
            print(change_x, change_y)
            change_threshold = 5

            # Detect huge changes, unlikely a valid moving object
            if change_x > change_threshold or change_y > change_threshold:
                continue
        else:
            ax = 0
            ay = 0
            # Detect too fast objects, unlikely a valid moving body
            if vx > 5 or vy > 5:
                continue

        print(movement['vx'], movement['vy'])

        # Perform motion prediction calculation using steady acceleration, straight motion formula
        delta_x = t_plus * vx + (ax * t_plus * t_plus) / 2
        delta_y = t_plus * vy + (ay * t_plus * t_plus) / 2
        new_x = movement['pair'][3]['center'][0] + delta_x
        new_y = movement['pair'][3]['center'][1] + delta_y
        x_chg = delta_x / (max_x - min_x)
        y_chg = delta_y / (max_y - min_y)

        # Include only reasonable predictions that fit into the scene
        if min_x <= new_x <= max_x and min_y <= new_y <= max_y and x_chg < 0.5 and y_chg < 0.5:
            result.append(shift_box(movement['pair'][3]['box'], delta_x, delta_y))
            moving.append(movement['pair'][0])

    print(result)
    print(moving)
    return result, moving


def get_intersection(box1: list,
                     box2: list) -> Optional[tuple]:
    """
    Get intersection of the two boxes.

    :param box1: First box.
    :param box2: Second box.
    :return: Intersection box or None if no intersection.
    """
    # Unpack coordinates
    x1, y1, x2, y2 = box1
    x3, y3, x4, y4 = box2

    # Get "new" coordinates
    x2 += x1
    y2 += y1
    x4 += x3
    y4 += y3
    x5 = max(x1, x3)
    y5 = max(y1, y3)
    x6 = min(x2, x4)
    y6 = min(y2, y4)
    if x5 > x6 or y5 > y6:
        return
    return x5, y5, x6 - x5, y6 - y5


def predict_object_collisions(future_objects: list,
                              moving_objects: list,
                              all_objects: dict) -> list:
    """
    Predict collision in the entire scene.

    :param future_objects: Future objects positions.
    :param moving_objects: Current objects.
    :param all_objects: All objects boxes.
    :return: List of predicted collisions.
    """
    # Get moving objects index
    moving_objects_set = set(moving_objects)

    # Get static objects - objects not in moving objects index
    static_objects = [obj for i, obj in enumerate(all_objects['boxes']) if i not in moving_objects_set]

    collisions = []
    for future in future_objects:
        for box in static_objects:
            intersection = get_intersection(future, box)
            if intersection:
                print("Got collision:", intersection, future, box)
                collisions.append(intersection)
        # Used to efficiently track collision between moving objects
        static_objects.append(future)
    print(collisions)
    return collisions


def center_to_box(center: list) -> tuple:
    """
    Convert point to small box around it.

    :param center: Center point.
    :return: Point bounding box.
    """
    margin = 0.4
    return center[0] - margin, center[1] - margin, 2 * margin, 2 * margin


def consumer_loop() -> None:
    """
    Main Kafka consumer loop.

    Responsible for points fetching and prediction coordination.
    """
    print("Checking for points...")
    for message in consumer:
        print("%s:%d:%d: key=%s value=%s" % (message.topic,
                                             message.partition,
                                             message.offset,
                                             message.key,
                                             message.value))

        # Deserialize message
        points = json.loads(message.value.decode("utf-8"))

        print(f"Processing {len(points['points'])} points.")

        # Predictions coordination
        objects = detect_objects(points['points'])
        future_objects = []
        collisions = []
        if snapshots:
            pairs = detect_object_pairs(objects, snapshots[-1])
            movements = get_object_movements(pairs)
            frames.extend([center_to_box(x['pair'][3]['center']) for x in movements])
            global previous_frames
            future_objects, moving = predict_object_positions(movements, previous_frames, objects)
            previous_frames = {str(v['pair'][3]['center']): [v['vx'], v['vy']] for v in movements}
            collisions = predict_object_collisions(future_objects, moving, objects)

        # Save and visualize prediction
        snapshots.append(objects)
        update(points['points'], objects['boxes'], frames, future_objects, collisions)

        consumer.commit()
        break
    print("Finished checking")


if __name__ == '__main__':
    timer.timeout.connect(consumer_loop)
    timer.start(0)
    import sys

    if (sys.flags.interactive != 1) or not hasattr(QtCore, 'PYQT_VERSION'):
        QtGui.QApplication.instance().exec_()
