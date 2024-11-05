from owlready2 import get_ontology, default_world, sync_reasoner, Imp, Thing, onto_path
import dill
import itertools

class Ontology(object):
    _instance = None        
    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls.ontology = get_ontology("ontology_merged.rdf").load()
            cls._instance = super(Ontology, cls).__new__(
                                cls, *args, **kwargs)
        return cls._instance

class Pipeline(object):
    def __init__(self) -> None:
        self.function_store = {}
        self.pipelines = []

    def _search(self, subpart, pertain=None):
        store = {}
        inputs = subpart.expects
        for i in inputs:
            for d in subpart.usesData:
                try:
                    ps = d.pertain
                    if pertain is not None:
                        flag=True
                        for p in ps:
                           if p.name == pertain:
                               flag=False
                               break
                        if flag:
                           continue
                        #print(flag, d.pertain[0].name, pertain)
                except:
                    None
                    #print("No pertain")
                
                if i in d.returns:
                    if i not in store:
                        store[i] = []
                    if len(d.returns)>1:
                        store[i].append((eval(d.pythonDefinition[0]),None, d.returns.index(i), d.name))
                    else:
                        store[i].append((eval(d.pythonDefinition[0]),None, None, d.name))

        for i in inputs:
            if i not in store:
                for d in subpart.usesExtractor:
                    if i in d.returns:
                        sub_store = self._search(d, pertain)
                        try:
                            ps = d.pertain
                            if pertain is not None:
                                flag=True
                                for p in ps:
                                    if p.name == pertain:
                                        flag=False
                                        break
                                if flag:
                                    continue
                                #print(flag, d.pertain[0].name, pertain)
                        except:
                            None
                            #print("No pertain")
                        if len(sub_store)>0:
                            if i not in store:
                                store[i] = []
                            #####
                            try:
                                [sub_store[o] for o in d.expects]
                            except:
                                continue

                            for combo in [list(x) for x in itertools.product(*[sub_store[o] for o in d.expects])]:
                                if len(d.returns)>1:
                                    store[i].append((eval(d.pythonDefinition[0]), combo, d.returns.index(i),d.name))
                                else:
                                    store[i].append((eval(d.pythonDefinition[0]), combo, None,d.name))
                        else:
                            continue
        return store

    def search_combo(self, combo, parts):
        if isinstance(combo[1], list):
            parts.append(combo[3])
            if combo[2] is not None:
                self.search_combo(combo[1][combo[2]], parts)
            else:
                self.search_combo(combo[1][0], parts)
        else:
            parts.append(combo[3])
        return parts


    
    def search(self, pertain=None):
        default_world.as_rdflib_graph().serialize(destination="test.ttl")
        sync_reasoner(infer_property_values = True)
        models = default_world.sparql('SELECT ?s WHERE {?s a <http://example.com/ModelBuilder>}')
        for m in models:
            store = self._search(m[0], pertain)
            inputs = m[0].expects

            incomplete_pipeline = False
            for i in inputs:
                if i not in store:
                    incomplete_pipeline=True
                    break
            if incomplete_pipeline:
                continue
            else:
                duplicate_detect = set()
                for combo in [list(x) for x in itertools.product(*[store[d] for d in m[0].expects])]:
                    alpha = tuple([tuple(self.search_combo(x, [])) for x in combo])
                    if alpha not in duplicate_detect:
                        self.pipelines.append((eval(m[0].pythonDefinition[0]), combo))
                        duplicate_detect.add(alpha)

    
    def _execute(self, part):
        executed_store = []
        if part[1] is None:
            if part[2] is None:
                return dill.loads(part[0])()
            else:
                return dill.loads(part[0])()[part[2]]
        else:
            for s in part[1]:
                if type(s) is list:
                    if s[1] is not None:
                        executed_store.append(dill.loads(s[0])()[s[1]])
                    else:
                        executed_store.append(dill.loads(s[0])())
                else:
                    executed_store.append(self._execute(s))
            
            self.function_store[dill.loads(part[0]).__name__] = dill.loads(part[0])(*executed_store)
            if part[2] is None:
                return dill.loads(part[0])(*executed_store)
            else:
                return dill.loads(part[0])(*executed_store)[part[2]]

    def execute(self):
        models = []
        if len( self.pipelines)>0:
            input()
            for model in self.pipelines:
                executed_store = []
                for s in model[1]:
                    executed_store.append(self._execute(s))
                model = dill.loads(model[0])(*executed_store)
                print("############")
                models.append(model)
            return models#dill.loads(model[0])(*executed_store)
        else:
            print("Nothing to execute")
    
    def _predict(self, part, dct):
        data = {x.name:x for x in Thing.instances()}
        fname = dill.loads(part[0]).__name__

        if fname in data:
            if len(data[fname].expects)>0:
                inputs = [i.name for i in data[fname].expects]
                res = []
                for i in inputs:
                    if i in dct:
                        res.append(dct[i])
                if len(res) == len(data[fname].expects):
                    if part[2] is None:
                        if len(res)==1:
                            return dill.loads(part[0])(res[0])
                        else:
                            return dill.loads(part[0])(*res)
                    else:
                        if len(res)==1:
                            return dill.loads(part[0])(res[0])[part[2]]
                        else:
                            return dill.loads(part[0])(*res)[part[2]]
            
            ### To be checked!
            if len(data[fname].returns)>0:
                outputs = [o.name for o in data[fname].returns]
                res = []
                for o in outputs:
                    if o in dct:
                        res.append(dct[o])
                if len(res) == len(data[fname].returns):
                    if part[2] is None:
                        if len(res)==1:
                            return res[0]
                        else:
                            return res
                    else:
                        if len(res)==1:
                            return res[0]
                        else:
                            return res[part[2]]

        executed_store = []
        if part[1] is None:
            if part[2] is None:
                return dill.loads(part[0])()
            else:
                return dill.loads(part[0])()[part[2]]
        else:
            for s in part[1]:
                if type(s) is list:
                    if s[1] is not None:
                        executed_store.append(dill.loads(s[0])()[s[1]])
                    else:

                        executed_store.append(dill.loads(s[0])())
                else:
                    executed_store.append(self._predict(s, dct))
            
            if part[2] is None:
                return dill.loads(part[0])(*executed_store)
            else:
                return dill.loads(part[0])(*executed_store)[part[2]]



    def transform(self, dct):
        for model in self.pipelines:
            predicted_store = []
            for s in model[1]:
                predicted_store.append(self._predict(s, dct))
            return predicted_store

class Pertain():
    def __init__(self, name):
        self.name = name

def dataloader(*args, **kwargs):
    def inner(func):
        onto = Ontology().ontology
        ns = onto.get_namespace("http://example.com/")
        fdata = ns.DataLoader(func.__name__)
        fdata.pythonDefinition = [str(dill.dumps(func))]
        fno = onto.get_namespace("https://w3id.org/function/ontology#")
        if 'output' in kwargs:
                fparam = [fno.Parameter(o) for o in kwargs['output']]
                fdata.returns = fparam
        if 'pertain' in kwargs:
            #fparam = [onto.search_one(label=i) for i in kwargs['pertain']]
            fparam = [Pertain(x) for x in kwargs['pertain']]
            fdata.pertain = fparam
        
        return func
    # returning inner function    
    return inner

def featuretransformer(*args, **kwargs):
    def inner(func):
        onto = Ontology().ontology
        ns = onto.get_namespace("http://example.com/")
        fdata = ns.FeatureTransformer(func.__name__)
        fdata.pythonDefinition = [str(dill.dumps(func))]
        fno = onto.get_namespace("https://w3id.org/function/ontology#")
        if 'input' in kwargs:
            fparam = [fno.Parameter(i) for i in kwargs['input']]
            fdata.expects = fparam
        if 'output' in kwargs:
            fparam = [fno.Parameter(i) for i in kwargs['output']]
            fdata.returns = fparam
        if 'pertain' in kwargs:
            fparam = [Pertain(x) for x in kwargs['pertain']]
            #fparam = [onto.search_one(label=i) for i in kwargs['pertain']]
            fdata.pertain = fparam
        return func
    # returning inner function    
    return inner

def modelbuilder(*args, **kwargs):
    def inner(func):
        onto = Ontology().ontology
        ns = onto.get_namespace("http://example.com/")
        fdata = ns.ModelBuilder(func.__name__)
        fdata.pythonDefinition = [str(dill.dumps(func))]
        fno = onto.get_namespace("https://w3id.org/function/ontology#")
        if 'input' in kwargs:
            fparam = [fno.Parameter(i) for i in kwargs['input']]
            fdata.expects = fparam
        if 'pertain' in kwargs:
            #fparam = [onto.search_one(label=i) for i in kwargs['pertain']]
            fparam = [Pertain(x) for x in kwargs['pertain']]
            fdata.pertain = fparam
        return func
    # returning inner function    
    return inner