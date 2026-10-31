import logging

logging.getLogger("sla_monitor").addHandler(logging.NullHandler())   # keep test output quiet
logging.getLogger("sla_monitor").propagate = False
