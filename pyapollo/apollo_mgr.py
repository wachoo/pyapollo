from apollo.apollo_client import ApolloClient


class ApolloMgr:

    def __init__(self, app_id='APP_ID', server_url=None, namespaces=[]):
        self.client = ApolloClient(app_id=app_id, cluster='default', server_url=server_url)
        self.namespace_names = namespaces
        self.apollo_loop_start()

    def get(self, key=None, default=None):
        for name in self.namespace_names:
            v = self.client.get_value(key, default, namespace=name)
            if v is None:
                continue
            break
        return v

    def apollo_loop_start(self):
        self.client.start(use_process=True)

    def ucm_loop_stop(self):
        self.client.stop()
