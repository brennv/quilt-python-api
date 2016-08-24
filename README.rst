Convert files to live data sets on Quilt
========================================

Optional prep (your steps may vary)
-----------------------------------

#. Get a list of files you want to upload (see ``get-files-to-upload/``)
#. Download the files in the list (see ``curl-all.py``)
#. Unzip downloaded files (if needed)

   .. code:: bash

       cd downloads
       gunzip *.gz

   .. rubric:: Upload to Quilt
      :name: upload-to-quilt

#. | Use ``data_set.py`` to create individual data sets (see
     ``python data_set.py --help``).
   | You will need a Quilt username and password. Or use ``batch.py`` to
     create multiple data sets.

   .. code:: bash

       python data_set.py
         -e https://quiltdata.com
         -u USERNAME
         -n "ENCODE data"
         -d "#A549 #histone peak data #hg19"
         -f downloads/wgEncodeBroadHistoneNhaH3k36me3StdPk.broadPeak

File formats in this example
----------------------------

-  `ENCDOE broadPeak format`_

Resources
---------

-  `ENCODE Project`_

REST API
========

+--------------------------------+--------------------------------------+-------------------------------------------+
| Action                         | Endpoint                             | Details                                   |
+================================+======================================+===========================================+
| New table                      | ``POST /tables/``                    | `See below`_                              |
+--------------------------------+--------------------------------------+-------------------------------------------+
| Delete table                   | ``DELETE /tables/TABLE_ID/``         | `See below <#delete-table>`__             |
+--------------------------------+--------------------------------------+-------------------------------------------+
| Update table meta-data         | ``PATCH /tables/TABLE_ID``           | `See below <#update-table-meta-data>`__   |
+--------------------------------+--------------------------------------+-------------------------------------------+
| Add column to table            | ``POST /tables/TABLE_ID/columns/``   | `See below <#add-column-to-table>`__      |
+--------------------------------+--------------------------------------+-------------------------------------------+
| Append row to table            | ``POST /data/TABLE_ID/rows/``        | `See below <#append-row>`__               |
+--------------------------------+--------------------------------------+-------------------------------------------+
| Get table rows                 | ``GET /data/TABLE_ID/rows``          | `See below <#get-rows>`__                 |
+--------------------------------+--------------------------------------+-------------------------------------------+
| Get table row                  | ``GET /data/TABLE_ID/rows/ROW_ID``   | `See below <#get-row>`__                  |
+--------------------------------+--------------------------------------+-------------------------------------------+
| Genome intersect or subtract   | ``POST /genemath/``                  | `See below <#intersect-or-subtract>`__    |
+--------------------------------+--------------------------------------+-------------------------------------------+

Notes

-  For all REST calls, the content-type is ``application/JSON``.
-  Description fields automatically linkify URLs and support
   ``<a>, <i>, <em>, <strong>, <b>`` tags

Tables
------

Create table
~~~~~~~~~~~~

``POST /tables/``

Data format
^^^^^^^^^^^

.. code:: javascript

    {
      'name': string,
      'description': text `<a>, <i>, <em>, <strong>, <b>` tags supported; automatic linkification of URLs
      'columns': [
        {
          'name': string,
          'sqlname': optional string,
          'description': optional text,
          'type' : one of 'String', 'Number', 'Image', 'Text'
        },
        ...
      ]
    }

Returns
^^^^^^^

Table data as JSON object, includes ``id`` field with the table’s
identifier.

Add column to table
~~~~~~~~~~~~~~~~~~~

``POST /tables/TABLE_ID/columns/``

Data format
^^^^^^^^^^^

.. code:: javascript

    {
       'name': string,
       'sqlname': optional string,
       'description': text,
       'type': one of 'String', 'Number', 'Image', or 'Text'
    }

Returns
^^^^^^^

Column data as JSON object, includes ``id`` field with the column’s
identifier.

Delete table
~~~~~~~~~~~~

``DELETE /tables/TABLE_ID``

Update table meta-data
~~~~~~~~~~~~~~~~~~~~~~

``PATCH /tables/TABLE_ID``

Data format
^^^^^^^^^^^

.. code:: javascript

    {
       'name': string,
       'description': text
    }

Table Data
----------

-  Use column ``sqlname`` as keys in input data

Append row
~~~~~~~~~~

``POST /data/TABLE_ID/rows/``

Data format
^^^^^^^^^^^

| \`\`\`javascript
| [
| {columnSqlname0: value0, c

.. _ENCDOE broadPeak format: https://genome.ucsc.edu/FAQ/FAQformat.html#format13
.. _ENCODE Project: https://www.encodeproject.org/
.. _See below: #create-table
