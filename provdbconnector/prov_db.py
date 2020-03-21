import json
import logging
import os
from collections import namedtuple
from io import StringIO
from uuid import uuid4

from prov.constants import PROV_ATTRIBUTES, PROV_MENTION, PROV_BUNDLE, PROV_LABEL, PROV_TYPE
from prov.model import ProvDocument, ProvEntity, ProvBundle, ProvRecord, ProvElement, ProvRelation, QualifiedName, \
    ProvAssociation, PROV_REC_CLS, ProvActivity, ProvAgent, PROV_AGENT,PROV_ENTITY,PROV_ACTIVITY, PROV_ATTR_AGENT,PROV_ATTR_ACTIVITY, PROV_ATTR_ENTITY,PROV_ATTR_BUNDLE
from provdbconnector.db_adapters.baseadapter import METADATA_KEY_PROV_TYPE, METADATA_KEY_IDENTIFIER, \
    METADATA_KEY_NAMESPACES, \
    METADATA_KEY_TYPE_MAP, METADATA_KEY_IDENTIFIER_ORIGINAL
from provdbconnector.exceptions.provapi import NoDataBaseAdapterException, InvalidArgumentTypeException, \
    InvalidProvRecordException
from provdbconnector.exceptions.utils import ParseException
from provdbconnector.exceptions.database import NotFoundException
from provdbconnector.utils.converter import form_string, to_json, to_provn, to_xml
from provdbconnector.utils.serializer import encode_json_representation, add_namespaces_to_bundle, create_prov_record, \
    PROV_ATTR_BASE_CLS, serialize_namespace

LOG_LEVEL = os.environ.get('LOG_LEVEL', '')
NUMERIC_LEVEL = getattr(logging, LOG_LEVEL.upper(), None)
logging.basicConfig(level=NUMERIC_LEVEL)
logging.getLogger("prov.model").setLevel(logging.WARN)
log = logging.getLogger(__name__)

PROV_API_BUNDLE_IDENTIFIER_PREFIX = "prov:bundle:{}"


class ProvDb(object):
    """
    The public api class. This class provide methods to save and get documents or part of ProvDocuments

    """

    def __init__(self, api_id=None, adapter=None, auth_info=None, *args):
        """
        Save a new instance of ProvAPI

        :param api_id: The id of the api, optional
        :type api_id: int or str
        :param adapter: The adapter class, must enhance from BaseAdapter
        :type adapter: Baseadapter
        :param auth_info: A dict object that contains the information for authentication
        :type auth_info: dict or None
        """
        if api_id is None:
            self.api_id = uuid4()
        else:
            self.api_id = api_id

        if adapter is None:
            raise NoDataBaseAdapterException()
        self._adapter = adapter()
        self._adapter.connect(auth_info)

    # Converter Methods
    def save_document_from_json(self, content=None):
        """
        Saves a new document in the database

        :param content: The content
        :type content: str or buffer
        :return: document_id
        :rtype: str or buffer
        """
        prov_document = form_string(content=content)
        return self.save_document(content=prov_document)

    def get_document_as_json(self, document_id=None):
        """
        Get a ProvDocument from the database based on the document_id

        :param document_id: document id
        :type document_id: str
        :return: ProvDocument as json string
        :rtype: str
        """
        prov_document = self.get_document_as_prov(document_id=document_id)
        return to_json(prov_document)

    def save_document_from_xml(self, content=None):
        """
        Saves a prov document in the database based on the xml file

        :param content: The content
        :type content: str or buffer
        :return: document_id
        :rtype: str
        """
        prov_document = form_string(content=content)
        return self.save_document(content=prov_document)

    def get_document_as_xml(self, document_id=None):
        """
        Get a ProvDocument from the database based on the document_id

        :param document_id: The id
        :type document_id: str
        :return: ProvDocument as XML string
        :rtype: str
        """
        prov_document = self.get_document_as_prov(document_id=document_id)
        return to_xml(prov_document)

    def save_document_from_provn(self, content=None):
        """
        Saves a prov document in the database based on the provn string or buffer

        :param content: provn object
        :type content: str or buffer
        :return: Document_id
        :rtype: str
        """
        prov_document = form_string(content=content)
        return self.save_document(content=prov_document)

    def get_document_as_provn(self, document_id=None):
        """
        Get a ProvDocument from the database based on the document_id

        :param document_id: The id
        :type document_id: str
        :return: ProvDocument
        :rtype: ProvDocument
        """
        prov_document = self.get_document_as_prov(document_id=document_id)
        return to_provn(prov_document)

    def save_document_from_prov(self, content=None):
        """
        Saves a prov document in the database based on the prov document

        :param content: Prov document
        :type content: ProvDocument
        :return: document_id
        :rtype: str
        """
        if not isinstance(content, ProvDocument):
            raise InvalidArgumentTypeException()
        return self.save_document(content=content)

    # Methods that consume ProvDocument instances and produce ProvDocument instances
    def save_document(self, content=None):
        """
        The main method to Save a document in the db

        :param content: The content can be a xml, json or provn string or buffer or a ProvDocument instance
        :type content: str or buffer or ProvDocument
        :return: Document id
        :rtype: str
        """

        # Try to convert the content into the provDocument, if it is already a ProvDocument instance the function will return this document
        try:
            content = form_string(content=content)
        except ParseException as e:
            raise InvalidArgumentTypeException(e)

        prov_document = content

        doc_id = self._save_bundle_internal(prov_document)

        for bundle in prov_document.bundles:
            self.save_bundle(prov_bundle=bundle)

        return doc_id

    def get_document_as_prov(self, document_id=None):
        """
        Get a ProvDocument from the database based on the document id

        :param document_id: The id
        :type document_id: str
        :return: Prov Document
        :rtype: ProvDocument
        """
        if type(document_id) is not str:
            raise InvalidArgumentTypeException()

        filter_meta = dict()
        filter_prop = dict()
        filter_meta.update({document_id: True})
        filter_prop.update({PROV_TYPE: PROV_BUNDLE})

        bundle_entities = self._adapter.get_records_by_filter(metadata_dict=filter_meta, attributes_dict=filter_prop)
        document_records = self._adapter.get_records_by_filter(metadata_dict=filter_meta)

        # parse document
        prov_document = ProvDocument()
        for record in document_records:
            self._parse_record(prov_document, record)

        bundle_doc = ProvDocument()# Document with all bundle entities

        for bundle_record in bundle_entities:

            # skip if we got some relations instead of only the bundle nodes
            if str(PROV_TYPE) not in bundle_record.attributes:
                continue

            if str(bundle_record.attributes[str(PROV_TYPE)]) != str(PROV_BUNDLE):
                continue

            bundle_entity= self._parse_record(bundle_doc,bundle_record)
            prov_bundle = self.get_bundle(bundle_entity.identifier)
            prov_document.add_bundle(prov_bundle,identifier=bundle_entity.identifier)


        return prov_document

    def save_element(self, prov_element, bundle_id=None):
        """
        Saves a activity, entity, agent

        .. code:: python


            doc = ProvDocument()

            agent       = doc.agent("ex:yourAgent")
            activity    = doc.activity("ex:yourActivity")
            entity      = doc.entity("ex:yourEntity")

            # Save the elements
            agent_id = prov_db.save_element(agent)
            activity_id = prov_db.save_element(activity)
            entity_id = prov_db.save_element(entity)


        :param prov_element: The ProvElement
        :type prov_element: prov.model.ProvElement
        :param bundle_id:
        :type bundle_id: str
        :return: Identifier of the element
        :rtype: prov.model.QualifiedName
        """
        if not isinstance(prov_element, ProvElement):
            raise InvalidArgumentTypeException("Should be {} but was {}".format(ProvElement, type(prov_element)))

        (metadata, attributes) = self._get_metadata_and_attributes_for_record(prov_element, bundle_id=bundle_id)
        self._adapter.save_element(attributes=attributes, metadata=metadata)

        #Add bundle relation only if the record belongs to a bundle not to document
        if not isinstance(prov_element.bundle, ProvDocument):
            bundle_id_qualified = prov_element.bundle.valid_qualified_name(prov_element.bundle.identifier)
            self._create_bundle_association([prov_element], bundle_id_qualified)

        return prov_element.identifier

    def get_elements(self, prov_element_cls):
        """
        Return a document that contains the requested type

        .. code:: python

            from prov.model import ProvEntity, ProvAgent, ProvActivity

            document_with_all_entities = prov_db.get_elements(ProvEntity)
            document_with_all_agents = prov_db.get_elements(ProvAgent)
            document_with_all_activities = prov_db.get_elements(ProvActivity)

            print(document_with_all_entities)
            print(document_with_all_agents)
            print(document_with_all_activities)


        :param prov_element_cls:
        :return: Prov document
        :rtype prov.model.ProvDocument

        """
        if prov_element_cls is ProvAgent:
            prov_type = PROV_AGENT
        elif prov_element_cls is ProvActivity:
            prov_type = PROV_ACTIVITY
        elif prov_element_cls is ProvEntity:
            prov_type = PROV_ENTITY
        else:
            raise InvalidArgumentTypeException("You provide a wrong type : {}".format(type(prov_element_cls)))

        meta_filter = dict()
        meta_filter.update({METADATA_KEY_PROV_TYPE: prov_type})

        doc = ProvDocument()
        raw_results = self._adapter.get_records_by_filter(metadata_dict=meta_filter)

        for element in raw_results:
            if element.metadata[METADATA_KEY_PROV_TYPE] == str(prov_type):
                self._parse_record(doc,element)
        return doc

    def get_element(self, identifier):
        """
        Get a element (activity, agent, entity) from the database

        .. code:: python

            doc = ProvDocument()

            identifier = QualifiedName(doc, "ex:yourAgent")

            prov_element = prov_db.get_element(identifier)


        :param identifier:
        :type identifier: prov.model.QualifiedName
        :return: A prov Element class
        """

        if not isinstance(identifier, QualifiedName):
            raise InvalidArgumentTypeException("Should be {} but was {}".format(QualifiedName, type(identifier)))

        # Include namespace uri into the identifier to support e.g. different default namespaces
        global_identifier = identifier.namespace.uri + identifier.localpart

        # Setup filter
        meta_filter = dict()
        meta_filter.update({METADATA_KEY_IDENTIFIER: global_identifier})

        # Get the result
        results = self._adapter.get_records_by_filter(metadata_dict=meta_filter)

        # Check if there is some unexpected result
        if len(results) > 1:
            raise InvalidProvRecordException("Invalid data result, len should be only one, result was: {}".format(list(results)))
        if len(results) == 0:
            raise NotFoundException("Can't find the element with identifier {}".format(identifier))

        # get the lonely element in the list
        element = list(results).pop()

        doc = ProvDocument()
        return self._parse_record(doc,element)

    def save_record(self, prov_record, bundle_id=None):
        """
        Saves a realtion or a element (Entity, Agent or Activity)

        .. code:: python


            doc = ProvDocument()

            agent       = doc.agent("ex:Alice")
            ass_rel     = doc.association("ex:Alice", "ex:Bob")

            # Save the elements
            agent_id = prov_db.save_record(agent)
            relation_id = prov_db.save_record(ass_rel)

        :param prov_record: The prov record
        :type prov.model.ProvRecord
        :param bundle_id: The bundle id that you got back if you created a bundle or document
        :type str
        :return:
        """
        if not isinstance(prov_record,ProvRecord):
            raise InvalidArgumentTypeException("Wrong type, expected: {}, got {}".format(type(ProvRecord), type(prov_record)))

        if isinstance(prov_record,ProvRelation):
            return self.save_relation(prov_relation=prov_record,bundle_id=bundle_id)
        elif isinstance(prov_record,ProvElement):
            return self.save_element(prov_element=prov_record,bundle_id=bundle_id)
        else:
            raise InvalidArgumentTypeException("Oh no... you provided a not supported prov_record type. The type was: {}".format(type(prov_record)))

    @staticmethod
    def _parse_record(prov_bundle, raw_record):
        """
        This method Saves a ProvRecord in the ProvBundle based on the raw database response

        :param prov_bundle: ProvBundle instance
        :type prov_bundle: ProvBundle
        :param raw_record: DbRelation or DbRecord instance as (namedtuple)
        :type raw_record: DbRelation or DbRecord
        :return ProvRecord
        :rtype prov.model.ProvRecord
        """

        # check if record belongs to this bundle
        prov_type = raw_record.metadata[METADATA_KEY_PROV_TYPE]
        prov_type = prov_bundle.valid_qualified_name(prov_type)

        # skip connections between bundle entities and all records that belong to the bundle
        prov_label = raw_record.attributes.get(str(PROV_LABEL))
        if prov_label is not None and prov_label == "belongsToBundle":
            return

        prov_id = raw_record.metadata[METADATA_KEY_IDENTIFIER_ORIGINAL]
        prov_id_qualified = prov_bundle.valid_qualified_name(prov_id)

        # skip record if prov:identifier starts with "prov:Unknown"
        if str(prov_id).startswith("prov:Unknown-"):
            return

        # set identifier only if it is not a prov type
        if prov_id_qualified == prov_type:
            prov_id = None

        # get type map
        type_map = raw_record.metadata[METADATA_KEY_TYPE_MAP]

        if type(type_map) is str:
            io = StringIO(type_map)
            type_map = json.load(io)

        elif type(type_map) is list:
            type_map_list = type_map
            type_map = dict()
            for type_str in type_map_list:
                if type(type_str) is not str:
                    raise InvalidArgumentTypeException("The type_map must be a string got: {}".format(type_str))
                io = StringIO(type_str)
                type_map.update(json.load(io))

        elif type(type_map) is not dict:
            raise InvalidArgumentTypeException("The type_map must be a dict or json string got: {}".format(type_map))

        add_namespaces_to_bundle(prov_bundle, raw_record.metadata)
        return create_prov_record(prov_bundle, prov_type, prov_id, raw_record.attributes, type_map)

    def get_bundle(self, identifier):
        """
        Returns the whole bundle for the provided identifier

        .. code:: python
            doc = ProvDocument()
            bundle_name = doc.valid_qualified_name("ex:YourBundleName")
            # get the bundle
            prov_bundle = prov_db.get_bundle(bundle_name)
            doc.add_bundle(prov_bundle)


        :param identifier: The identifier
        :type identifier: prov.model.QualifiedName
        :return: The prov bundle instance
        :rtype prov.model.ProvBundle
        """
        if not isinstance(identifier, QualifiedName):
            raise InvalidArgumentTypeException()


        bundle_entity = self.get_element(identifier)

        doc = ProvDocument()
        doc.add_record(bundle_entity)#Add bundle entity to document

        prov_bundle = doc.bundle(identifier=bundle_entity.identifier)

        # Include namespace uri into the identifier to support e.g. different default namespaces
        global_identifier = identifier.namespace.uri + identifier.localpart
        bundle_records = self._adapter.get_bundle_records(global_identifier)

        for record in bundle_records:
            self._parse_record(prov_bundle, record)

        return prov_bundle

    def save_bundle(self,prov_bundle):
        """
        Public method to save a bundle

        .. code:: python

            doc = ProvDocument()

            bundle = doc.bundle("ex:bundle1")
            # Save the bundle
            prov_db.save_bundle(bundle)

        :param prov_bundle:
        :type prov_bundle: prov.model.ProvBundle
        :return:
        """

        if not isinstance(prov_bundle, ProvBundle):
            raise InvalidArgumentTypeException()
        if isinstance(prov_bundle, ProvDocument):
            raise  InvalidArgumentTypeException()

        # create bundle entity
        bundle_record = ProvEntity(prov_bundle.document, identifier=prov_bundle.identifier, attributes={PROV_TYPE: PROV_BUNDLE})
        self.save_element(prov_element=bundle_record)

        return self._save_bundle_internal(prov_bundle)

    def _save_bundle_internal(self, prov_bundle):
        """
        Private method to create a bundle in the database

        :param prov_bundle: ProvBundle
        :type prov_bundle: prov.model.ProvBundle
        :return bundle_id: The bundle from the database adapter
        :rtype: str
        """
        if not isinstance(prov_bundle, ProvBundle):
            raise InvalidArgumentTypeException()

        bundle_id = str(uuid4())
        # create nodes
        for record in prov_bundle.get_records(ProvElement):
            self.save_element(prov_element=record, bundle_id=bundle_id)

        # create relations
        for relation in prov_bundle.get_records(ProvRelation):

            self.save_relation(relation, bundle_id)

        return bundle_id

    def save_relation(self, prov_relation, bundle_id=None):
        """
        Saves a relation between 2 nodes that are already in the database.

        .. code:: python

            doc = ProvDocument()

            activity    = doc.activity("ex:yourActivity")
            entity      = doc.entity("ex:yourEntity")
            wasGeneratedBy = entity.wasGeneratedBy("ex:yourAgent")

            # Save the elements
            rel_id = prov_db.save_relation(wasGeneratedBy)


        :param prov_relation: The ProvRelation instance
        :type prov_relation: ProvRelation
        :param bundle_id
        :type bundle_id: str
        :return: Relation id
        :rtype: str
        """

        if not isinstance(prov_relation, ProvRelation):
            raise InvalidArgumentTypeException(
                "prov_relation was {}, expected: {}".format(type(prov_relation), type(ProvRelation)))


        # get from and to node
        from_tuple, to_tuple = prov_relation.formal_attributes[:2]
        from_qualified_name = from_tuple[1]
        to_qualified_name = to_tuple[1]

        # if target or origin record is unknown, save node "Unknown"
        if from_qualified_name is None:
            uid = uuid4()
            from_qualified_name = prov_relation.bundle.valid_qualified_name("prov:Unknown-{}".format(uid))
            del uid

        if to_qualified_name is None:
            uid = uuid4()
            to_qualified_name = prov_relation.bundle.valid_qualified_name("prov:Unknown-{}".format(uid))
            del uid

        # Ensure that the from and to node exists
        relation_cls = PROV_REC_CLS[prov_relation.get_type()]
        from_type,to_type = relation_cls.FORMAL_ATTRIBUTES[:2]

        # get the class types
        from_type_cls = PROV_ATTR_BASE_CLS[from_type]
        to_type_cls = PROV_ATTR_BASE_CLS[to_type]

        if from_type_cls is None or to_type_cls is None:
            raise InvalidArgumentTypeException(
                "Could not determinate typ for relation from: {}, to: {}, prov_relation was {}, ".format(from_type, to_type, type(prov_relation)))
        #save from and to node
        self.save_element(prov_element=from_type_cls(prov_relation.bundle, identifier=from_qualified_name), bundle_id=bundle_id)

        to_bundle = prov_relation.bundle

        # If it is a link between bundle the to node not belongs to the current bundle, the to node belongs only to the bundle defined as FORMAL_ATTR[3]
        if prov_relation.get_type() is PROV_MENTION:

            #Try to get the destination bundle
            to_bundle_identifier = list(prov_relation.get_attribute(PROV_ATTR_BUNDLE)).pop()

            if not isinstance(to_bundle_identifier,QualifiedName):
                raise InvalidProvRecordException("Should be a qualified name {}, mention: {}".format(to_bundle_identifier, prov_relation))
            # Create the bundle, it will be automatically created during the save_element method
            to_bundle = ProvBundle(identifier=to_bundle_identifier)


        self.save_element(prov_element=to_type_cls(to_bundle, identifier=to_qualified_name), bundle_id=bundle_id)


        # split metadata and attributes
        (metadata, attributes) = self._get_metadata_and_attributes_for_record(prov_relation)

        # Include namespace uri into the identifier to support e.g. different default namespaces
        global_from_qualified_name = from_qualified_name.namespace.uri + from_qualified_name.localpart
        global_to_qualified_name = to_qualified_name.namespace.uri + to_qualified_name.localpart

        return self._adapter.save_relation(global_from_qualified_name, global_to_qualified_name,
                                           attributes, metadata)

    def _create_bundle_association(self, prov_elements, prov_bundle_identifier):
        """
        This method saves a relation between the bundle entity and all nodes in the bundle

        :param prov_bundle_identifier: The bundle identifier
        :type prov_bundle_identifier: QualifiedName
        :param prov_elements: List of prov elements
        :type prov_elements: list
        """

        # Ensure that the bundle entity exist
        doc = ProvDocument()
        to_bundle = ProvBundle(document=doc,identifier=prov_bundle_identifier)
        self.save_bundle(to_bundle) # Save the empty bundle to create the bundle entity if necessary

        belong_relation = ProvAssociation(bundle=to_bundle, identifier=None,
                                          attributes={PROV_TYPE: "prov:bundleAssociation"})
        (belong_metadata, belong_attributes) = self._get_metadata_and_attributes_for_record(belong_relation)
        to_qualified_name = prov_bundle_identifier

        for record in prov_elements:
            (metadata, attributes) = self._get_metadata_and_attributes_for_record(record)
            from_qualified_name = metadata[METADATA_KEY_IDENTIFIER]
            global_prov_to_identifier= to_qualified_name.namespace.uri + to_qualified_name.localpart
            self._adapter.save_relation(from_qualified_name, global_prov_to_identifier,
                                        belong_attributes, belong_metadata)


    def _save_bundle_links(self, prov_bundle):
        """
        This function saves the links between nodes in bundles, see https://www.w3.org/TR/prov-links/

        :param prov_bundle: For this bundle we will create the links
        :type prov_bundle: ProvBundle
        """

        for mention in prov_bundle.get_records(ProvRelation):
            if mention.get_type() is not PROV_MENTION:
                continue

            self.save_relation(mention)

    @staticmethod
    def _get_metadata_and_attributes_for_record(prov_record, bundle_id=None):
        """
        This function generate some meta data for the record for example:

            * Namespaces: The prov_record use several namespaces and the metadata contain this namespaces
            * Type_Map: The type map is important to get exactly the same document back, you have to save this information (like what attribute is a datetime)

        :param prov_record: The ProvRecord (ProvRelation or ProvElement)
        :type prov_record: ProvRecord
        :param bundle_id: The id of the document
        :type bundle_id: str
        :return: namedtuple(metadata, attributes)
        :rtype: namedtuple
        """
        if not isinstance(prov_record, ProvRecord):
            raise InvalidArgumentTypeException()

        used_namespaces = dict()
        bundle = prov_record.bundle

        prov_type = prov_record.get_type()
        prov_identifier = prov_record.identifier

        if prov_type is None and isinstance(prov_record, ProvRecord):
            prov_type = bundle.valid_qualified_name("prov:Unknown")

        # if relation without identifier -> use prov_type as identifier
        if prov_identifier is None and prov_record.identifier is None:
            prov_identifier = prov_type

        # Be sure that the prov_identifier is a qualified name instance

        if not isinstance(prov_identifier, QualifiedName):
            qualified_name = bundle.valid_qualified_name(prov_identifier)
            if qualified_name is None:
                raise InvalidProvRecordException(
                    "The prov record {} is invalid because the prov_identifier {} can't be qualified".format(
                        prov_record, prov_identifier))
            else:
                prov_identifier = qualified_name

        # extract namespaces from record

        # add namespace from prov_type
        namespace = prov_type.namespace
        used_namespaces.update(serialize_namespace(namespace))

        # add namespace from prov identifier
        namespace = prov_identifier.namespace
        used_namespaces.update(serialize_namespace(namespace))

        attributes = dict(prov_record.attributes.copy())
        for key, value in attributes.items():

            # ensure key is QualifiedName
            if isinstance(key, QualifiedName):
                namespace = key.namespace
                used_namespaces.update(serialize_namespace(namespace))
            else:
                raise InvalidProvRecordException("Not support key type {}".format(type(key)))

            # try to add
            if isinstance(value, QualifiedName):
                namespace = value.namespace
                used_namespaces.update(serialize_namespace(namespace))
            else:
                qualified_name = bundle.valid_qualified_name(value)
                if qualified_name is not None:
                    # Don't update the attribute, so we only save the namespace instead of the attribute as a qualified name.
                    # For some reason the prov-library allow a string with a schema: <namespace_prefix>:<identifier>
                    # This line cause an error during the test: "test_primer_example_alternate"
                    # attributes[key] = qualified_name # update attribute

                    namespace = qualified_name.namespace
                    used_namespaces.update(serialize_namespace(namespace))

        # create type dict
        types_dict = dict()
        for key, value in attributes.items():
            if key not in PROV_ATTRIBUTES:
                return_type = encode_json_representation(value)
                if return_type is not None:
                    types_dict.update({str(key): return_type})

        # Include namespace uri into the identifier to support e.g. different default namespaces
        global_prov_identifier = prov_identifier.namespace.uri + prov_identifier.localpart

        metadata = {
            METADATA_KEY_PROV_TYPE: prov_type,
            METADATA_KEY_IDENTIFIER: global_prov_identifier,
            METADATA_KEY_IDENTIFIER_ORIGINAL: prov_identifier,
            METADATA_KEY_NAMESPACES: used_namespaces,
            METADATA_KEY_TYPE_MAP: types_dict
        }

        # Add document id to metadata, to restore the
        if bundle_id:
            metadata.update({bundle_id: True})
        meta_and_attributes = namedtuple("MetaAndAttributes", "metadata, attributes")

        return meta_and_attributes(metadata, attributes)
