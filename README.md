supervisor-joblogger
====================

Supervisor plugin to log job start/stop/exit events to a (sqlite) DB.

Configuring the eventlistener
-----------------------------
Configure the plugin as an event listener in the /etc/supervisor/supervisord.conf:

[eventlistener:joblogger]
command=/usr/bin/joblogger.py
events=PROCESS_STATE
autostart=true

Configuring a maximum runtime
-----------------------------

The plugin can be configured to kill commands if they run over a certain amount
of time.  Set `maxruntime` to the timedelta and ensure `autorestart` is false.

[program:good]
command=myscript.sh
autorestart=false
maxruntime=10m
