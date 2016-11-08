import json
import logging
import os
from collections import namedtuple
from io import StringIO
from uuid import uuid4

from prov.constants import PROV_ATTRIBUTES, PROV_MENTION, PROV_BUNDLE, PROV_LABEL, PROV_TYPE
from prov.model import ProvDocument, ProvEntity, ProvBundle, ProvRecord, ProvElement, ProvRelation, QualifiedName, \
    ProvAssociation
from provdbconnector.db_adapters.baseadapter import METADATA_KEY_PROV_TYPE, METADATA_KEY_IDENTIFIER, \
    METADATA_KEY_NAMESPACES, \
    METADATA_KEY_TYPE_MAP
from provdbconnector.exceptions.provapi import NoDataBaseAdapterException, InvalidArgumentTypeException, \
    InvalidProvRecordException
from provdbconnector.exceptions.utils import ParseException
from provdbconnector.utils.converter import form_string, to_json, to_provn, to_xml
from provdbconnector.utils.serializer import encode_json_representation, add_namespaces_to_bundle, create_prov_record

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
        Create a new instance of ProvAPI

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
    def create_document_from_json(self, content=None):
        """
        Creates a new document in the database

        :param content: The content
        :type content: str or buffer
        :return: document_id
        :rtype: str or buffer
        """
        prov_document = form_string(content=content)
        return self.create_document(content=prov_document)

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

    def create_document_from_xml(self, content=None):
        """
        Creates a prov document in the database based on the xml file

        :param content: The content
        :type content: str or buffer
        :return: document_id
        :rtype: str
        """
        prov_document = form_string(content=content)
        return self.create_document(content=prov_document)

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

    def create_document_from_provn(self, content=None):
        """
        Creates a prov document in the database based on the provn string or buffer

        :param content: provn object
        :type content: str or buffer
        :return: Document_id
        :rtype: str
        """
        prov_document = form_string(content=content)
        return self.create_document(content=prov_document)

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

    def create_document_from_prov(self, content=None):
        """
        Creates a prov document in the database based on the prov document

        :param content: Prov document
        :type content: ProvDocument
        :return: document_id
        :rtype: str
        """
        if not isinstance(content, ProvDocument):
            raise InvalidArgumentTypeException()
        return self.create_document(content=content)

    # Methods that consume ProvDocument instances and produce ProvDocument instances
    def create_document(self, content=None):
        """
        The main method to create a document in the db

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

        doc_id = self._create_bundle(prov_document)

        for bundle in prov_document.bundles:
            bundle_record = ProvEntity(bundle, identifier=bundle.identifier, attributes={PROV_TYPE: PROV_BUNDLE})
            (metadata, attributes) = self._get_metadata_and_attributes_for_record(bundle_record, bundle_id=doc_id)
            self._adapter.save_record(attributes=attributes, metadata=metadata)

            self._create_bundle(bundle)
            self._create_bundle_association(prov_elements=bundle.get_records(ProvElement),
                                            prov_bundle_identifier=bundle.identifier)

        for bundle in prov_document.bundles:
            self._create_bundle_links(bundle)

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

        for bundle_record in bundle_entities:

            # skip if we got some relations instead of only the bundle nodes
            if str(PROV_TYPE) not in bundle_record.attributes:
                continue

            if str(bundle_record.attributes[str(PROV_TYPE)]) != str(PROV_BUNDLE):
                continue

            identifier = bundle_record.metadata[METADATA_KEY_IDENTIFIER]
            prov_bundle = prov_document.bundle(identifier=identifier)

            bundle_records = self._adapter.get_bundle_records(identifier)

            for record in bundle_records:
                self._parse_record(prov_bundle, record)
        return prov_document

    @staticmethod
    def _parse_record(prov_bundle, raw_record):
        """
        This method creates a ProvRecord in the ProvBundle based on the raw database response

        :param prov_bundle: ProvBundle instance
        :type prov_bundle: ProvBundle
        :param raw_record: DbRelation or DbRecord instance as (namedtuple)
        :type raw_record: DbRelation or DbRecord
        """

        # check if record belongs to this bundle
        prov_type = raw_record.metadata[METADATA_KEY_PROV_TYPE]
        prov_type = prov_bundle.valid_qualified_name(prov_type)

        # skip record if prov:type "prov:Unknown"
        if prov_type is prov_bundle.valid_qualified_name("prov:Unknown"):
            return

        # skip connections between bundle entities and all records that belong to the bundle
        prov_label = raw_record.attributes.get(str(PROV_LABEL))
        if prov_label is not None and prov_label == "belongsToBundle":
            return

        prov_id = raw_record.metadata[METADATA_KEY_IDENTIFIER]
        prov_id_qualified = prov_bundle.valid_qualified_name(prov_id)

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
        create_prov_record(prov_bundle, prov_type, prov_id, raw_record.attributes, type_map)

    def _create_bundle(self, prov_bundle):
        """
        Private method to create a bundle in the database

        :param prov_bundle: ProvBundle
        :type prov_bundle: prov.model.ProvBundle
        :return bundle_id: The bundle from the database adapter
        :rtype: str
        """
        bundle_id = str(uuid4())

        if not isinstance(prov_bundle, ProvBundle):
            raise InvalidArgumentTypeException()

        # create nodes
        for record in prov_bundle.get_records(ProvElement):
            (metadata, attributes) = self._get_metadata_and_attributes_for_record(record, bundle_id)
            self._adapter.save_record(attributes, metadata)

        # create relations
        for relation in prov_bundle.get_records(ProvRelation):
            # skip relations of the type "prov:mentionOf" https://www.w3.org/TR/prov-links/
            if relation.get_type() is PROV_MENTION:
                continue

            self._create_relation(relation, bundle_id, prov_bundle.identifier)

        return bundle_id

    def _create_relation(self, prov_relation, bundle_id=None, bundle_identifier=None):
        """
        Creates a relation between 2 nodes that are already in the database.

        :param prov_relation: The ProvRelation instance
        :type prov_relation: ProvRelation
        :param bundle_id
        :type bundle_id: str
        :param bundle_identifier
        :type bundle_identifier: str
        :return: Relation id
        :rtype: str
        """
        # get from and to node
        from_tuple, to_tuple = prov_relation.formal_attributes[:2]
        from_qualified_name = from_tuple[1]
        to_qualified_name = to_tuple[1]

        # if target or origin record is unknown, create node "Unknown"
        if from_qualified_name is None:
            from_qualified_name = self._create_unknown_node(bundle_identifier=bundle_identifier, bundle_id=bundle_id)

        if to_qualified_name is None:
            to_qualified_name = self._create_unknown_node(bundle_identifier=bundle_identifier, bundle_id=bundle_id)

        # split metadata and attributes
        (metadata, attributes) = self._get_metadata_and_attributes_for_record(prov_relation)
        return self._adapter.save_relation(from_qualified_name, to_qualified_name,
                                           attributes, metadata)

    def _create_bundle_association(self, prov_elements, prov_bundle_identifier):
        """
        This method creates a relation between the bundle entity and all nodes in the bundle

        :param prov_bundle_identifier: The bundle identifier
        :type prov_bundle_identifier: str
        :param prov_elements: List of prov elements
        :type prov_elements: list
        """
        bundle = ProvBundle()
        belong_relation = ProvAssociation(bundle=bundle, identifier=None,
                                          attributes={PROV_TYPE: "prov:bundleAssociation"})
        (belong_metadata, belong_attributes) = self._get_metadata_and_attributes_for_record(belong_relation)
        to_qualified_name = prov_bundle_identifier

        for record in prov_elements:
            (metadata, attributes) = self._get_metadata_and_attributes_for_record(record)
            from_qualified_name = metadata[METADATA_KEY_IDENTIFIER]
            self._adapter.save_relation(from_qualified_name, to_qualified_name,
                                        belong_attributes, belong_metadata)

    def _create_unknown_node(self, bundle_identifier=None, bundle_id=None):
        """
        If a relation end or start is "Unknown" (yes this is allowed in PROV) we create a specific node to create the relation

        :param bundle_identifier: The bundle identifier
        :type bundle_identifier: str
        :param bundle_id: The bundle id
        :type bundle_id: str
        :return: The identifier of the Unknown node
        :rtype: str
        """
        uid = uuid4()
        doc = ProvDocument()
        identifier = doc.valid_qualified_name("prov:Unknown-{}".format(uid))
        record = ProvRecord(bundle=doc, identifier=identifier)

        (metadata, attributes) = self._get_metadata_and_attributes_for_record(record, bundle_id)
        self._adapter.save_record(attributes, metadata)

        # Create bundle association for unknown node if this node is for a bundle, ugly but working
        if bundle_identifier is not None:
            self._create_bundle_association([record], bundle_identifier)

        return identifier

    def _create_bundle_links(self, prov_bundle):
        """
        This function creates the links between nodes in bundles, see https://www.w3.org/TR/prov-links/

        :param prov_bundle: For this bundle we will create the links
        :type prov_bundle: ProvBundle
        """

        for mention in prov_bundle.get_records(ProvRelation):
            if mention.get_type() is not PROV_MENTION:
                continue

            self._create_relation(mention)

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
        used_namespaces.update({str(namespace.prefix): str(namespace.uri)})

        # add namespace from prov identifier
        namespace = prov_identifier.namespace
        used_namespaces.update({str(namespace.prefix): str(namespace.uri)})

        attributes = dict(prov_record.attributes.copy())
        for key, value in attributes.items():

            # ensure key is QualifiedName
            if isinstance(key, QualifiedName):
                namespace = key.namespace
                used_namespaces.update({str(namespace.prefix): str(namespace.uri)})
            else:
                raise InvalidProvRecordException("Not support key type {}".format(type(key)))

            # try to add
            if isinstance(value, QualifiedName):
                namespace = value.namespace
                used_namespaces.update({str(namespace.prefix): str(namespace.uri)})
            else:
                qualified_name = bundle.valid_qualified_name(value)
                if qualified_name is not None:
                    # Don't update the attribute, so we only save the namespace instead of the attribute as a qualified name.
                    # For some reason the prov-library allow a string with a schema: <namespace_prefix>:<identifier>
                    # This line cause an error during the test: "test_primer_example_alternate"
                    # attributes[key] = qualified_name # update attribute

                    namespace = qualified_name.namespace
                    used_namespaces.update({str(namespace.prefix): str(namespace.uri)})

        # create type dict
        types_dict = dict()
        for key, value in attributes.items():
            if key not in PROV_ATTRIBUTES:
                return_type = encode_json_representation(value)
                if return_type is not None:
                    types_dict.update({str(key): return_type})

        metadata = {
            METADATA_KEY_PROV_TYPE: prov_type,
            METADATA_KEY_IDENTIFIER: prov_identifier,
            METADATA_KEY_NAMESPACES: used_namespaces,
            METADATA_KEY_TYPE_MAP: types_dict
        }

        # Add document id to metadata, to restore the
        if bundle_id:
            metadata.update({bundle_id: True})
        meta_and_attributes = namedtuple("MetaAndAttributes", "metadata, attributes")

        return meta_and_attributes(metadata, attributes)
