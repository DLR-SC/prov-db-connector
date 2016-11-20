Changelog
=========

Version 0.3
-----------

-  **Changed ``provdb.create_*`` to ``provdb.save_*``** because we can’t
   guarantee that the db-adapter actual create a new node, document,
   relation. Maybe the adapter merges your properties into existing
   data, behavior is still the same.
-  **Renamed files provDb.py into prov\_db.py**
-  Enhanced the ``prov:Mention`` support. If you create a bundle link
   (``prov:Mention``) the destination bundle entity will be
   automatically created. For example: \`\`\`python

from prov.tests.examples import bundles2

doc = bundles2() bundle = list(doc.get\_records()).pop() #I know, the
get\_record function return a set, so it can happen that you get the
wrong bundle here (alice:bundle5 is correct)
prov\_api.save\_bundle(bundle) \`\`\`

-  Add ability to save relations between elements that doesn’t exist.
   For example, on a empty database:

.. code:: python

    doc = ProvDocument()
    relation = doc.wasGeneratedBy("ex:Entity", "ex:Activity")

    #Works now fine. The ex:entity and ex:Activity elements will be created automatically 
    provapi.save_relation(relation)

-  Removed node type “Unknown” for relations with unknown nodes. (The
   prov-db-adapter now detects which type the relation implicitly mean.

.. code:: python

    doc = ProvDocument()
    relation = doc.wasGeneratedBy("ex:Entity", -)

    # Creates a Activity with a random identifier as destions for the relation  
    provapi.save_relation(relation)

-  Introduced new methods

**prov\_db.save\_relation(prov\_relation)**

.. code:: python


    doc = ProvDocument()

    activity    = doc.activity("ex:yourActivity")
    entity      = doc.entity("ex:yourEntity")
    wasGeneratedBy = entity.wasGeneratedBy("ex:yourAgent")

    # Save the elements
    rel_id = prov_db.save_relation(wasGeneratedBy)

**prov\_db.save\_element(prov\_element, [bundle\_id])**

.. code:: python


    doc = ProvDocument()

    agent       = doc.agent("ex:yourAgent")
    activity    = doc.activity("ex:yourActivity")
    entity      = doc.entity("ex:yourEntity")

    # Save the elements
    agent_id = prov_db.save_element(agent)
    activity_id = prov_db.save_element(activity)
    entity_id = prov_db.save_element(entity)

**prov\_db.get\_element(identifier)**

.. code:: python


    doc = ProvDocument()

    identifier = QualifiedName(doc, "ex:yourAgent")

    prov_element = prov_db.get_element(identifier)

**prov\_db.save\_record(prov\_record, [bundle\_id])**

.. code:: python


    doc = ProvDocument()

    agent       = doc.agent("ex:Alice")
    ass_rel     = doc.association("ex:Alice", "ex:Bob")

    # Save the elements
    agent_id = prov_db.save_record(agent)
    relation_id = prov_db.save_record(ass_rel)

**prov\_api.save\_bundle(prov\_bundle)**

.. code:: python


    doc = ProvDocument()

    bundle = doc.bundle("ex:bundle1")
    # Save the bundle
    prov_db.save_bundle(bundle)

**prov\_db.get\_elements([ProvCLS])**

.. code:: python

    from prov.model import ProvEntity, ProvAgent, ProvActivity

    document_with_all_entities = prov_db.get_elements(ProvEntity)
    document_with_all_agents = prov_db.get_elements(ProvAgent)
    document_with_all_activities = prov_db.get_elements(ProvActivity)

    print(document_with_all_entities)
    print(document_with_all_agents)
    print(document_with_all_activities)

**prov\_db.get\_bundle(identifier)**

.. code:: python

    doc = ProvDocument()
    bundle_name = doc.valid_qualified_name("ex:YourBundleName")
    # get the bundle
    prov_bundle = prov_db.get_bundle(bundle_name)
    doc.add_bundle(prov_bundle)