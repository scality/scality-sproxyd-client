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
1. Download the archive (ZIP or tar.gz) of the source code from the 
   `releases page`_.

.. _releases page: https://github.com/scality/scality-sproxyd-client/releases

2. Install the package:

   .. code-block:: console

       sudo pip install scality-sproxyd-client-X.X.X.tar.gz
