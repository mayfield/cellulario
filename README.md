cellulario
===========

_*Cellular IO - A self-contained and self-managed IO loop*_

[![Maturity](https://img.shields.io/pypi/status/cellulario.svg)](https://pypi.python.org/pypi/cellulario)
[![License](https://img.shields.io/pypi/l/cellulario.svg)](https://pypi.python.org/pypi/cellulario)
[![Change Log](https://img.shields.io/badge/change-log-blue.svg)](https://github.com/mayfield/cellulario/blob/master/CHANGELOG.md)
[![Build Status](https://semaphoreci.com/api/v1/projects/3a285086-d1cf-4585-97ec-6b96e707b0c9/595977/shields_badge.svg)](https://semaphoreci.com/mayfield/cellulario)
[![Version](https://img.shields.io/pypi/v/cellulario.svg)](https://pypi.python.org/pypi/cellulario)

About
--------

Cellular IO is an interface for IO loop style programming that bundles the IOloop
lifecycle into the inner cell of a standard python data interface.  If the cell
is used like a generator then the ioloop is managed inside the generator during
calls to `__next__`.  If callbacks are used then the cell will block the current
execution context until the sum of IO routines are complete.

An IOCell can consist of multiple levels of cascading IO routines.  A first tier
may do requests that add requests of a second tier to the workqueue.  The cell
coordinator keeps track of the different io tiers to ensure they are scheduled
according to the strategy chosen by the user, be it latency, bandwidth, or just
FIFO.


Installation
--------

    python3 ./setup.py build
    python3 ./setup.py install


Compatibility
--------

* Python 3.5.3+
