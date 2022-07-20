from .dynamo import DynamoClient


class Variables:

    def __init__(self, name, country):
        self.name = name
        self.country = country
        self.dynamocli = DynamoClient(self.country)

    def get_vars(self, name):
        return self.dynamocli.get_vars(name)

    def update_vars(self, name, new_value):
        return self.dynamocli.update_vars(name, new_value)

    def delete_vars(self, name):
        return self.dynamocli.delete_vars(name)

    def search_vars(self, expr):
        return self.dynamocli.search_vars(expr)

    def search_values(self, expr):
        return self.dynamocli.search_values(expr)

    def get_values(self, name):
        return self.dynamocli.get_values(name)
