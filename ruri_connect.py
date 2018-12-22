from pymongo import MongoClient

class ConnectTo:
    def __init__(self, host, port, database, collection):
        self.__host = host
        self.__port = port
        self.__database = database
        self.__collection = collection

        # mongoDB
        self.__m_client = ""
        self.__m_database = ""
        self.__m_collection = ""

    @property
    def host(self):
        return self.__host

    @host.setter
    def host(self, value):
        self.__host = value

    @property
    def port(self):
        return self.__port

    @port.setter
    def port(self, value):
        self.__port = value

    @property
    def database(self):
        return self.__database

    @database.setter
    def database(self, value):
        self.__database = value

    @property
    def collection(self):
        return self.__collection

    @collection.setter
    def collection(self, value):
        self.__collection = value

    @property
    def m_client(self):
        return self.__m_client

    @m_client.setter
    def m_client(self, value):
        self.__m_client = value
    
    @property
    def m_database(self):
        return self.__m_database

    @m_database.setter
    def m_database(self, value):
        self.__m_database = value

    @property
    def m_collection(self):
        return self.__m_collection

    @m_collection.setter
    def m_collection(self, value):
        self.__m_collection = value

    #현재는 mongoDB만 가능하지만 추후 다른 DB도 선택할 수 있도록 함수 추가

    def MongoDB(self):
        self.__m_client = MongoClient(self.__host, self.__port)
        self.__m_database = self.m_client[self.__database]
        self.__m_collection = self.__m_database[self.__collection]
