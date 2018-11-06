Sproxyd client for Scality Sproxyd Rest Connector
========================================================

This package implements a client, written in Python, for the Scality_ Sproxyd Rest Connector.
It supports the HEAD/GET/POST/DELETE HTTP methods.

The client's constructor can be initialized with URLs to several Sproxyd connectors and 
has a periodic health check based failure detector so that only healthy Sproxyd connectors
are accessed.

.. _Scality: http://scality.com

Installation
------------
   .. code-block:: console

       pip install scality-sproxyd-client
