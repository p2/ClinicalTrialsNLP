#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#	Utilities for Geocoding
#
#	2013-07-15	Created by Pascal Pfiffner
#

from math import sin		# faster than using math.sin!
from math import asin
from math import cos
from math import sqrt
from math import pi


def km_distance_between(lat1, lng1, lat2, lng2):
	""" Distance in kilometers between the two given points, using the
	Haversine formula. """
	
	earth_rad = 6371
	dLat = _deg2rad(lat2 - lat1)
	dLon = _deg2rad(lng2 - lng1)
	a = sin(dLat/2) * sin(dLat/2) + cos(_deg2rad(lat1)) * cos(_deg2rad(lat2)) * sin(dLon/2) * sin(dLon/2)
	c = 2 * asin(sqrt(a))
	
	return earth_rad * c


def _deg2rad(deg):
	return deg * (pi / 180)

