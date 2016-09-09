#!/usr/bin/env python

import os
import unittest

from gsps.nc import load_configs


class TestLoadConfigs(unittest.TestCase):

    def test_load_config(self):
        test_path = os.path.join(
            os.path.dirname(__file__),
            'resources'
        )
        configs = load_configs(test_path)

        assert 'usf-bass' in configs

        assert 'global_attributes' in configs['usf-bass']
        assert 'deployment' in configs['usf-bass']
        assert 'instruments' in configs['usf-bass']
