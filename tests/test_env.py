from icc import env
import os


class TestEnv():
    def test_get(self):
        os.environ['TRUE'] = "TRUE"
        os.environ['True'] = 'True'
        os.environ['true'] = 'true'
        os.environ['FALSE'] = "FALSE"
        os.environ['False'] = 'False'
        os.environ['false'] = 'false'
        os.environ['String'] = "String"

        for i in ('TRUE', 'True', 'true'):
            assert env.get(i) == True
        for i in ('FALSE', 'False', 'false'): 
            assert env.get(i) == False
        assert env.get('String') == 'String'
        assert env.get('Nothing') == None
