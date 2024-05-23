#!/bin/bash

# stop manager and iengine
supervisorctl -c supervisor/supervisord.conf stop iengine
supervisorctl -c supervisor/supervisord.conf stop manager

# stop supervisord
supervisorctl -c supervisor/supervisord.conf shutdown
