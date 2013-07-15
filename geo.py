#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#	Utilities for Geocoding
#
#	2013-07-15	Created by Pascal Pfiffner
#

import math


def km_distance_between(lat1, lng1, lat2, lng2):
	""" Distance in kilometers between the two given points, using the
	Haversine formula. """
	
	earth_rad = 6371;
	dLat = _deg2rad(lat2 - lat1);
	dLon = _deg2rad(lng2 - lng1); 
	a = math.sin(dLat/2) * math.sin(dLat/2) + math.cos(_deg2rad(lat1)) * math.cos(_deg2rad(lat2)) * math.sin(dLon/2) * math.sin(dLon/2);
	c = 2 * math.asin(math.sqrt(a));
	
	return earth_rad * c;


def _deg2rad(deg):
	return deg * (math.pi / 180)

