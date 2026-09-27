"""Configuration pytest commune.

Force le fuseau UTC avant le demarrage de Spark : PySpark interprete les datetime
Python sans fuseau dans le fuseau du systeme. Sans cela, les tests d'heures
(pickup_hour...) dependent de la machine qui les execute.
"""
from __future__ import annotations

import os
import time

os.environ["TZ"] = "UTC"
time.tzset()