from pymongo import MongoClient


class ConnectTo:
    def __init__(self, host, port, database, collection, username, password):
        self.__host = host
        self.__port = port
        self.__database = database
        self.__collection = collection
        self.__username = username
        self.__password = password

        # mongoDB
        self.__m_client = None
        self.__m_database = None
        self.__m_collection = None

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

    @property
    def username(self):
        return self.__username

    @username.setter
    def username(self, value):
        self.__username = value

    @property
    def password(self):
        return self.__password

    @password.setter
    def password(self, value):
        self.__password = value

    # 현재는 mongoDB만 가능하지만 추후 다른 DB도 선택할 수 있도록 함수 추가

    def MongoDB(self):
        self.__m_client = MongoClient(
            host=self.__host, port=self.__port, username=self.__username, password=self.__password)
        self.__m_database = self.m_client[self.__database]
        self.__m_collection = self.__m_database[self.__collection]
