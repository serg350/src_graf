import re
import copy
import importlib as imp

from comsdk.graph import Graph, Func, State, Selector
from comsdk.edge import Edge


class Params():
    __slots__ = (
        'module', 'entry_func', 'predicate', 'selector', 'function',
        'morphism', 'parallelism', 'comment', 'order', 'subgraph',
        'keys_mapping', 'executable_parameters', 'connection_data',
        'preprocessor', 'postprocessor', 'edge_index', 'config_section',
    )

    def __init__(self):
        for slot in self.__slots__:
            setattr(self, slot, None)
        self.comment = ""  # Инициализируем как пустую строку вместо None

    def __str__(self):
        stri = ""
        for s in self.__slots__:
            attr = getattr(self, s)
            if attr is not None and s != 'comment':  # Комментарии выводим отдельно
                stri += f"{s}: {attr}, "
        if self.comment:
            stri += f"comment: '{self.comment}'"
        return stri

# entities = {}

class GraphFactory():
    __slots__ = (
        'name',
        'states', 
        'graph',
        'issub',
        'tocpp',
        'entities'
    )
    def __init__(self, tocpp=False):
        self.states = {}
        self.entities = {}
        self.tocpp = tocpp
        self.name = None
        self.issub = False

    def add_state(self, statename):
        if statename not in self.states:
            self.states[statename] = State(statename)
            if statename in self.entities:
                self.states[statename].comment = self.entities[statename].comment
    
    def _create_morphism(self, morphname=None):
        comment = ""
        if morphname is None:
            return Func(), Func(), comment
        pred_f, func_f = Func(), Func() 
        morph = self.entities[morphname]
        for m in morph.__slots__:
            if getattr(morph,m) is not None:
                if m!="predicate" and m!="function" and m!="comment":
                    raise Exception("ERROR: Morphisms could not have any params exept comment, predicate and function!\n{}".format(morphname))
                if m=="comment":
                    comment=getattr(morph, m).replace("\0", " ")
                if m=="predicate":
                    if getattr(morph,m) not in self.entities:
                        raise Exception("\tERROR: Predicate {} is not defined!".format(getattr(morph, m)))
                    pred = self.entities[getattr(morph, m)]
                    if self.tocpp:
                        pred_f = Func(pred.module, pred.entry_func, dummy=True, comment=pred.comment)
                    else:
                        pred_f = Func(pred.module, pred.entry_func, comment=pred.comment)
                if m=="function":
                    if getattr(morph,m) not in self.entities:
                       raise Exception("\tERROR: Function: {} is not defined!".format(getattr(morph, m)))
                    fu = self.entities[getattr(morph, m)]
                    if self.tocpp:
                        func_f = Func(fu.module, fu.entry_func, dummy=True, comment=fu.comment)
                    else:
                        func_f = Func(fu.module, fu.entry_func,comment=fu.comment)
        return pred_f, func_f, comment


    def add_connection(self, st1, st2, morphism=None, ordr=0):
        pred, entr, comm = self._create_morphism(morphism)
        self.states[st1].connect_to(self.states[st2], edge=Edge(pred, entr, order=ordr, comment=comm))
        print("{} -> {}".format(st1, st2))

    def build(self, nsub):
        print("BUILDING {}\nStates:".format(self.name))
        for s in self.states:
            print("\t"+ s)
        if self.issub:
            self.graph = Graph(self.states[self.name+str(nsub)+"_"+"__BEGIN__"], self.states[self.name+str(nsub)+"_"+"__END__"])
        else:    
            self.graph = Graph(self.states["__BEGIN__"], self.states["__END__"])
        self.graph.init_graph()
        if self.issub:
            oldkeys = []
            for e in self.entities:
                oldkeys.append(e)
            for old in oldkeys:
                if self.entities[old].selector is not None or self.entities[old].subgraph is not None:
                    self.entities[self.name + str(Parser.subgr_count)+"_"+old] = self.entities[old]
                    del self.entities[old] 
        for s in self.states:
            if s in self.entities and self.entities[s].selector is not None:
                selname = self.entities[s].selector
                if self.tocpp:
                    self.states[s].selector = Selector(len(self.states[s].transfers), self.entities[selname].module, self.entities[selname].entry_func, dummy=True)
                else:
                    self.states[s].selector = Selector(len(self.states[s].transfers), self.entities[selname].module, self.entities[selname].entry_func)
            else:
                self.states[s].selector =  Selector(len(self.states[s].transfers))
            if s in self.entities and self.entities[s].subgraph is not None:
                print("Replacing state {} with subgraph {}".format(s,self.entities[s].subgraph))
                parsr = Parser(subgraph=True, tocpp= self.tocpp)
                subgr = parsr.parse_file(self.entities[s].subgraph)
                self.states[s].replace_with_graph(subgr)
                self.graph = Graph(self.graph.init_state, self.graph.term_state)
                print(self.graph)
        return self.graph


class Parser():
    __slots__ = (
        'fact',
        'issub'
    )
    subgr_count = 0
    def __init__(self, tocpp=False, subgraph=False):
        self.fact = GraphFactory(tocpp=tocpp)
        self.fact.issub = subgraph
        self.issub = subgraph
        if subgraph:
            Parser.subgr_count+=1
    
    def _check_brackets(self, rawfile):
        br = { "[":{"line":0, "count":0}, "(":{"line":0, "count":0}, "{":{"line":0, "count":0}, "\"":{"line":0, "count":0}}
        line = 1
        qu = 0
        for char in rawfile:
            if char == "[":
                br["["]["line"] = line
                br["["]["count"] +=1 
            elif char == "{":
                br["{"]["line"] = line
                br["{"]["count"] +=1 
            elif char == "(":
                br["("]["line"] = line
                br["("]["count"] +=1 
            elif char == "]":
                br["["]["count"] -=1 
            elif char == "}":
                br["{"]["count"] -=1 
            elif char == ")":
                br["("]["count"] -=1 
            elif char =="\"":
                br["\""]["line"] = line
                br["\""]["count"] += 1 if br["\""]["count"]==0 else -1
            elif char == "\n":
                line+=1
        expstr= "Brackets or quotes do not match! Missing closing brackets on lines: "
        fl = False
        for c in br:
            if br[c]["count"] != 0:
                fl= True
                expstr+=str(br[c]["line"])+" "
        if fl:
            raise Exception(expstr)

    def _split_multiple(self,param):
        vals = {}
        first=True
        for s in param.__slots__:
            attr = getattr(param,s)
            if attr is not None and '\0' in attr:
                vals[s] = attr.split('\0')
        l=0
        for sl in vals:
            if l==0:
                l=len(vals[sl])
            elif l!=len(vals[sl]):
                raise Exception("\tERROR: Number of multiple params do not match", l)
        res = [copy.copy(param) for i in range(l)]
        for sl in vals:
            for i, _ in enumerate(res):
                setattr(res[i], sl, vals[sl][i])
        return res

    def _param_from_props(self, props):
        parm = Params()
        comment = ""
        if props == "":
            return parm
        props = props.replace("]", '')

        # Извлекаем комментарий, но не удаляем его полностью
        if '\"' in props:
            matches = [m for m in re.finditer(r'\"(.*?)\"', props)]
            if matches:
                comment = matches[0].group(1).replace("\0", " ")
                # Заменяем комментарий на специальный маркер, чтобы не мешал дальнейшему парсингу
                props = props[:matches[0].start()] + "COMMENT_PLACEHOLDER" + props[matches[0].end():]

        if '(' in props:
            mchs = [m for m in re.finditer(r'\((\w+,)*\w+\)', props)]
            for m in mchs:
                props = props[:m.span()[0]] + (props[m.span()[0]:m.span()[1]]).replace(',', '\0') + props[m.span()[1]:]

        props = props.replace("(", "")
        props = props.replace(")", "")

        # Восстанавливаем комментарий после обработки скобок
        props = props.replace("COMMENT_PLACEHOLDER", "")

        rs = props.split(r",")
        for r in rs:
            r = r.split(r"=", 1)
            if r[0] in parm.__slots__:
                setattr(parm, r[0], r[1])
            else:
                raise Exception("\tERROR:Unknown parameter: " + r[0])

        if comment != "":
            setattr(parm, "comment", comment)
        return parm

    #Props is line "[proFp=smth, ...]"
    #def _param_from_props(self,props):
        #        parm = Params()
        #comment = ""
        #if props =="":
        #    return parm
        #props = props.replace("]", '')
        #if '\"' in props:
            # удаляеться комментарий
        #    m = [m for m in re.finditer(r'\".*\"', props)][0]
        #    comment = props[m.span()[0]+1:m.span()[1]-1]
        #    props=props[:m.span()[0]]+props[m.span()[1]:]
        #if '(' in props:
        #   mchs = [m for m in re.finditer(r'\((\w+,)*\w+\)', props)]
        #   for m in mchs:
        #       props=props[:m.span()[0]]+(props[m.span()[0]:m.span()[1]]).replace(',','\0')+props[m.span()[1]:]
        #props = props.replace("(","")
        #props = props.replace(")","")
        #rs =props.split(r",") #.split(r", ")
        #for r in rs:
        #    r=r.split(r"=", 1)
        #    if r[0] in parm.__slots__:
        #        setattr(parm, r[0], r[1])
        #    else:
        #       raise Exception("\tERROR:Unknown parameter: "+ r[0])
        #if comment != "":
        #    setattr(parm, "comment", comment.replace("\0", " "))
        #return parm

    def _param_from_entln(self, raw):
        res = re.split(r"\[", raw, 1)
        return res[0], self._param_from_props(res[1])

    def _multiple_morphs(self,props, n):
        p = self._param_from_props(props)
        if p.morphism is None:
            return  [copy.copy(p) for i in range(n)] 
        else:
            return self._split_multiple(p)

    def _topology(self,raw):
        spl = re.split(r"\s*(=>|->|\[|\])\s*", raw)
        spl = list(filter(lambda x: x!="[" and x!="]" and x!="", spl))
        left = spl[0].split(",")
        right = spl[2].split(",")
        if self.issub:
            for i in range(len(left)):
                left[i] = self.fact.name + str(Parser.subgr_count) + "_" + left[i]
            for i in range(len(right)):
                right[i] = self.fact.name + str(Parser.subgr_count) + "_" + right[i]
        if (len(left)>1) and (len(right)>1):
            raise Exception("ERROR: Ambigious multiple connection in line:\n\t{}".format(raw))
        # many to one conection
        elif len(left)>1:
            if len(spl) < 4:
                spl.append("")
            morphs = self._multiple_morphs(spl[3], len(left))
            if len(morphs)!=len(left):
                raise Exception("\tERROR: Count of edges do not match to count of states in many to one connection!\n\t\t{}".format(raw))
            self.fact.add_state(right[0])
            for i, st in enumerate(left):
                self.fact.add_state(st)
                self.fact.add_connection(st, right[0], morphs[i].morphism)
        # one to many connection, here could be selector
        elif len(right)>1:
            if len(spl) < 4:
                spl.append("")
            morphs = self._multiple_morphs(spl[3], len(right))
            self.fact.add_state(left[0])
            if len(morphs)!=len(right):
                raise Exception("\tERROR: Count of edges do not match to count of states in one to many connection!\n\t\t{}".format(raw))
            for i, st in enumerate(right):
                self.fact.add_state(st)
                self.fact.add_connection(left[0], st, morphs[i].morphism, morphs[i].order)
        # one to one connection
        else:
            self.fact.add_state(left[0])
            self.fact.add_state(right[0])
            if len(spl)==4:
                pr =self._param_from_props(spl[3])
                self.fact.add_connection(left[0], right[0], pr.morphism, ordr=pr.order if pr.order is not None else 0)
            elif len(spl)==3:
                self.fact.add_connection(left[0], right[0], None)

    def parse_file(self, filename):
        # @todo В случае, если на вход будет подан файл в отличной от UTF-8 кодировке программа работать не будет
        with open(filename, "r", encoding="utf-8") as file:
            dot = file.read()
        # проверка на правильное количесво скобок
        self._check_brackets(dot)

        # @todo Возможно стоит заменить данный код на библиотеку Lark
        # поиск всех подстрок в кавычках
        comments = [m for m in re.finditer(r'\".*\"', dot)]
        for m in comments:
            # Заменяет пробелы внутри кавычек на специальный символ \0
            dot=dot[:m.span()[0]]+(dot[m.span()[0]:m.span()[1]]).replace(' ','\0')+dot[m.span()[1]:]
        # Удаляет все пробелы, табы(\t) и возвраты каретки(\r), кроме тех, что внутри кавычек(они заменены на \0)
        dot = re.sub(r"[ \t\r]", "", dot) #deleting all spaces
        # Удаляет ключевые слова digraph, а также фигурные скобки { и }
        dot = re.sub(r"((digraph)|}|{)", "", dot)
        # Удаляет однострочные комментарии, начинающиеся с //
        dot = re.sub(r"\/\/.*", "", dot)
        # Удаляет строки, содержащие только перенос строки (\n)
        dot = re.sub(r"^\n$", "", dot)
        # @ todo заменить нижние две строки на dotlines = [line.strip() for line in dot.splitlines() if line.strip()]
        # разбивает строку на список подстрок
        dotlines = dot.splitlines()
        # фильтрует пустые подстроки
        dotlines = list(filter(None, dotlines))
        # записали имя графа в класс GraphFactory
        self.fact.name = dotlines[0]
        # Удалили из строки
        dotlines = dotlines[1:]
        # ent_re - regular expr for edges, states, functions properties
        # ищет строки вида ИМЯ[атрибуты]
        print(dotlines)
        ent_re = re.compile(r"^\w+\[.*\]$")
        # top_re - regular expr for topology properties, most time consuming one
        # Строки вида ИСТОЧНИК->ЦЕЛЬ[атрибуты] или ИСТОЧНИК=>ЦЕЛЬ[атрибуты]
        top_re = re.compile(r"^(\w+,?)+(->|=>)(\w+,?)+(\[(\w+=(\(?\w+,?\)?)+,?)+\])?")
        # (r"^\w[\w\s,]*(->|=>)\s*\w[\w\s,=\[\]()]*$")
        for i, ln in enumerate(dotlines):
            # функция
            if ent_re.match(ln):
                name, parm = self._param_from_entln(ln)
                print(f'{parm}')
                self.fact.entities[name] = parm
            # топология
            elif top_re.match(ln):
                self._topology(ln)
        return self.fact.build(Parser.subgr_count)

    checked=[]
    bushes = {}
    selectorends = {}

    def generate_cpp(self, filename=None):
        self.fact.graph.init_state.input_edges_number =0
        states_to_check = [self.fact.graph.init_state]
        while len(states_to_check)!=0:
            for st in states_to_check:
                self.checked.append(st)
                states_to_check.remove(st)
                bush = _Bush(st)
                bush.grow_bush()
                self.bushes[st] = bush
                for outs in bush.outstates:
                    if outs not in states_to_check and outs not in self.checked:
                        states_to_check.append(outs)
        send_token(self.fact.graph.init_state, self.bushes, [])
        preds, morphs, sels, st, body = print_graph(self.fact.graph.init_state, self.fact.entities, self.bushes)
        from mako.template import Template
        if filename is not None:
            f = open(filename, "w")
        else:
            f= open(self.fact.name + ".cpp", "w")
        print(Template(filename="./cpp/template.cpp").render(preds=preds, morphs = morphs, sels = sels, states=st, body=body), file=f)

def print_graph(cur_state, entities, bushes):
    checked = []
    toloadpred = []
    toloadmorph = []
    toloadsel =[]
    tocheck = [cur_state]
    body = ""
    while len(tocheck) !=0:
        cur_state=tocheck[0]
        cur_b = bushes[cur_state]
        cur_b.token+=1
        if cur_b.token < cur_b.state.input_edges_number - cur_b.state.looped_edges_number:
            tocheck.remove(cur_state)
            tocheck.append(cur_state)
            continue
        if cur_state in checked:
            tocheck.remove(cur_state)
            continue
        if len(cur_b.branches)>1 or len(cur_b.incomes)>1:
            body+="{}:\n".format(cur_state.name)
        if len(cur_b.incomes)!=0:
            if cur_b.state.comment!="" and cur_b.state.comment is not None:
                print("STcomm:", cur_b.state.comment)
                body+="//"+cur_b.state.comment+"\n"
            stri = "false "
            for inc in cur_b.incomes:
                stri += "|| SEL_{}[{}] ".format(inc["st"].name, inc["i"])
            body+="if (!({}))".format(stri)
            body+="{\n\tfor (int seli = 0;"+" seli < {};".format(len(cur_state.transfers))+" seli++)\n"+ "\t\tSEL_{}[seli]=false;".format(cur_state.name)+"\n}"
            if cur_state.selector.name != "":
                # print(cur_state.name, cur_state.selector)
                if cur_state.selector not in toloadsel:
                    toloadsel.append(cur_state.selector)
                body+="else {\n"+ "\tSEL_{} = {}(&data);//{}\n".format(cur_state.name, cur_state.selector, cur_state.selector.comment )+"}\n"
            else:
                body+="else {\n\tfor (int seli = 0;"+" seli < {};".format(len(cur_state.transfers))+" seli++)\n"+"\t\tSEL_{}[seli]=true;".format(cur_state.name)+"\n}\n"
        for i, br in enumerate(cur_b.branches):
            body+="if (SEL_{}[{}])".format(cur_state.name, i)+"{\n"
            if br[len(br)-1].output_state not in tocheck:
                tocheck.append(br[len(br)-1].output_state)
            if br[len(br)-1].output_state in checked or br[len(br)-1].output_state is cur_state:
                stri, toloadpred, toloadmorph = cur_b.cpp_branch(i, toloadpred, toloadmorph)
                body+=stri+"\tgoto {};\n".format(br[len(br)-1].output_state.name)+"}\n"
            else:
                stri, toloadpred, toloadmorph = cur_b.cpp_branch(i, toloadpred, toloadmorph) 
                body+=stri+"}\n"
        tocheck.remove(cur_state)
        checked.append(cur_state)
    return _unique(toloadpred), _unique(toloadmorph), _unique(toloadsel), checked, body

def _unique(lst):
    for i, el in enumerate(lst):
        for el2 in lst[i+1:]:
            if el2.module == el.module and el2.name == el.name:
                lst.remove(el2)
    return lst

def send_token(cur_state, bushes, checked):
    cur_b = bushes[cur_state]
    if cur_state in checked:
        return
    if len(cur_b.outstates)==0:
        return
    if len(cur_b.incomes) == cur_b.state.input_edges_number - cur_b.state.looped_edges_number:
        checked.append(cur_state)
        for i,br in enumerate(cur_b.branches):
            bushes[br[len(br)-1].output_state].incomes.append({"st":cur_state, "i":i})
            send_token(br[len(br)-1].output_state,bushes, checked)

class _Bush():
    __slots__=(
        'state', 
        'selector',
        'branches',
        'outstates',
        'token',
        'incomes',
        'selectorfin'
    )

    def __init__(self, state):
        self.state = state
        self.selector = state.selector
        self.branches = []
        self.outstates = []
        self.token = 0
        self.incomes = []

    def grow_bush(self):
        for t in self.state.transfers:
            branch = [t]
            self._gen_branch(t.output_state, branch)
        
    def _gen_branch(self, cur_state, branch):
        while len(cur_state.transfers)==1 and cur_state.input_edges_number==1:
            if cur_state._proxy_state is not None:
                cur_state=cur_state._proxy_state
            tr = cur_state.transfers[0]
            branch.append(tr)
            cur_state = tr.output_state
        self.branches.append(branch)
        if cur_state not in self.outstates:
            self.outstates.append(cur_state)
        
    def cpp_branch(self, i, toloadpred, toloadmorph):
        res = ""
        for tr in self.branches[i]:
            edge = tr.edge
            if edge.comment!="":
                res+="\t//{}\n".format(edge.comment)
            if edge.pred_f.name != "":
                if edge.pred_f not in toloadpred:
                    toloadpred.append(edge.pred_f)
                res+="\tcheck_pred({}(&data), \"{}\");".format(edge.pred_f, edge.pred_f)
                res+="//{}\n".format(edge.pred_f.comment) if edge.pred_f.comment != "" else "\n"
            if edge.morph_f.name != "":
                if edge.morph_f not in toloadmorph:
                    toloadmorph.append(edge.morph_f)
                res+="\t{}(&data);".format(edge.morph_f)
                res+="//{}\n".format(edge.morph_f.comment) if edge.morph_f.comment != "" else "\n"
        return res, toloadpred, toloadmorph


   

