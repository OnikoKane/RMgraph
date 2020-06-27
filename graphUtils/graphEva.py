from py2neo import Graph
import pandas as pd
url = 'localhost:7474'
username = 'neo4j'
password = 'neo4j'


'''
    remittanceAly: 对转账记录进行归纳，返回可能与传入personID有关联的personID list
    callAly: 对通话记录进行归纳，返回可能与传入personID有关联的personID list
    getAllUsers: 得到所有personID的list
    digRel: 调用remittanceAly和callAly，并对返回的personID list与原personID创建联系(同时更新到Neo4j)
    genData: 生成并导出数据
    还有一系列用于生成数据的辅助func，view层也可使用
'''


class GraphEva:
    def __init__(self):
        self.graph = Graph(url, username=username, password=password)

    '''
        建立ACKNOWLEDGE, 在digRel方法中进行统一调用
        从user的转账Remittance记录进行分析，并返回user实体之间可能存在相识关系的userID
        Input: personID
        Return: -1(没有可能存在的关系)
                list(可能存在关系的userID列表)
    '''

    def remittanceAly(self, value):
        remittanceRels = self.graph.run(
            "match (s:`person`)-[p:`remittance`]-(o:`person`) where s.personID= '{}' return p.amount, o.personID".format(
                value)).data()
        if len(remittanceRels) > 0:
            ackUsers = {}
            # 根据转账计数建立关系
            for rmt in remittanceRels:
                r = 1 if int(rmt['p.amount']) < 10000 else 2  # count
                if rmt['o.personID'] not in ackUsers:
                    ackUsers[rmt['o.personID']] = r
                else:
                    ackUsers[rmt['o.personID']] += r

            ackUsers = [user for user in ackUsers if ackUsers[user] > 1]
            # 最后需要建立关系的id!
            ackUsers = [user for user in ackUsers if
                        len(
                            self.graph.run(
                                "match (s:`person`)-[p]-(o:`person`) where type(p) in ['PARENT_OF','ACKNOWLEDGE','COLLEAGUE_OF'] and s.personID= '{}' and o.personID='{}' return type(p)"
                                    .format(value, user)).data())
                        == 0]
            if len(ackUsers) > 0:
                return ackUsers
            else:
                return -1
        else:
            return -1

    '''
        建立ACKNOWLEDGE, 在digRel方法中进行统一调用
        从user的转账Call记录进行分析，并返回user实体之间可能存在相识关系的userID
        Input: personID
        Return: -1(没有可能存在的关系)
                list(可能存在关系的userID列表)
    '''

    def callAly(self, value):
        '''
           流程：
           先得到该personID所持有的所有phone的ID
           查询与该phone有联系的所有phone的ID

           对满足通话次数阈值的phone查询其所有者(user)的ID
           对满足关系但是未建立关系的两个user进行记录并返回
        '''
        ownPhone = [i['o.phone'] for i in
                    self.graph.run(
                        "match (s:`person`)-[p:`ownPhone`]-(o:`phone`) where s.personID= '{}' return o.phone".format(
                            value))
                        .data()]
        if len(ownPhone) > 0:
            for phoneID in ownPhone:
                # get phone rel
                callRels = self.graph.run(
                    "match (s:`phone`)-[p:`call`]-(o:`phone`) where s.phone= '{}' return o.phone".format(
                        phoneID)).data()
                ackPhone = {}
                if len(callRels) > 0:
                    for call in callRels:
                        if call['o.phone'] not in ackPhone:
                            ackPhone[call['o.phone']] = 1
                        else:
                            ackPhone[call['o.phone']] += 1
                    # 根据通话次数进行筛选
                    ackPhone = [phone for phone in ackPhone if ackPhone[phone] > 1]

                    # ackPhone = ['phone_38', 'phone_35', 'phone_14', 'phone_9', 'phone_5'] # cache for test
                    # phone -> owner
                    if len(ackPhone) > 0:
                        ackUsers = []
                        for phoneID in ackPhone:

                            for i in self.graph.run(
                                    "match (s:`person`)-[p:`ownPhone`]-(o:`phone`) where o.phone= '{}' return s.personID".format(
                                        phoneID)).data():
                                # 避免重复建立关系
                                if len(
                                        self.graph.run(
                                            "match (s:`person`)-[p]-(o:`person`) where type(p) in ['PARENT_OF','ACKNOWLEDGE','COLLEAGUE_OF'] and s.personID= '{}' and o.personID='{}' return type(p)"
                                                    .format(value, i['s.personID'])).data()
                                ) == 0:
                                    ackUsers.append(
                                        i['s.personID']
                                    )
                        return ackUsers
                    else:
                        return -1
                else:
                    return -1
        else:
            return -1

    '''
        获取所有的User的ID
        Return: list
    '''

    def getAllUsers(self):
        users = self.graph.run("match (s:`person`)  return s.personID").data()
        return [i['s.personID'] for i in users]

    '''
        根据图数据分析创建新的关系，调用remittanAly和callAly两个方法
        Input: list (存有personID)
        Return: 1 (执行完成)
                -1 (没有执行，相应的userID不存在)
    '''

    def digRel(self, userlist):
        for value in userlist:
            userEntity = self.graph.run(
                "match (s:`person`) where s.personID= '{}' return s.personID".format(value)).data()
            if len(userEntity) > 0:
                unrels = []
                rmtRels = self.remittanceAly(value=value)
                callRels = self.callAly(value=value)

                if rmtRels != -1:
                    unrels += rmtRels
                if callRels != -1:
                    unrels += callRels
                # 将要创建链接的personID
                unrels = set(unrels)  # 消歧
                for userID in unrels:
                    self.graph.run(
                        "match (s:`person`{personID:'" + value + "'}),(o:`person`{personID:'" + userID + "'}) merge (s)-[:ACKNOWLEDGE]->(o)")
                return 1
            else:
                return -1

    '''
        ##### 以下方法用于生成隐藏数据 ##### 
        ##### 也可以作为前端使用 ##### 
    '''
    '''
        返回用户的flag和status
        status：rank D -> rank AA 
    '''

    def userInfo(self, value):
        return self.graph.run("match (s:`person`) where s.personID= '{}' return s.flag,s.status".format(value)).data()

    '''
        一层关系黑户人数
    '''

    def network1FlagNum(self, value):
        return len(self.graph.run(
            "match (s:`person`)-[p]-(o:`person`) where type(p) in ['PARENT_OF','ACKNOWLEDGE','COLLEAGUE_OF'] and o.flag='1' and s.personID= '{}' return o.personID".format(
                value)).data())

    '''
        二层关系黑户人数
    '''

    def network2FlagNum(self, value):
        return len(self.graph.run(
            "match (s:`person`)-[p]-(:`person`)-[q]-(o:`person`) where type(p) in ['PARENT_OF','ACKNOWLEDGE','COLLEAGUE_OF'] and type(q) in ['PARENT_OF','ACKNOWLEDGE','COLLEAGUE_OF'] and o.flag='1' and s.personID= '{}' return o.personID".format(
                value)).data())

    '''
        用户持有电话数量
    '''

    def userPhoneNum(self, value):
        return len(self.graph.run(
            "match (s:`person`)-[p:`ownPhone`]-(o:`phone`) where s.personID= '{}' return o.phone".format(value)).data())

    '''
        用户转账数目(金额>=4000)
    '''

    def userRmtNum(self, value):
        # 交易金额大于4000才计数
        # cache = self.graph.run(
        #     "match (s:`person`)-[p:`remittance`]-(o:`person`) where s.personID= '{}' return p.amount".format(
        #         value)).data()
        # return len([i['p.amount'] for i in cache if int(i['p.amount']) >= 4000])
        return len(self.graph.run(
            "match (s:`person`)-[p:`remittance`]-(o:`person`) where s.personID= '{}' and toInt(p.amount)>=4000 return p.amount".format(
                value)).data())

    '''
        返回用户tx的数目x3(违约、完结、被拒）
    '''

    def userTxConditionNum(self, value):
        defaultNum = len(self.graph.run(
            "match (s:`person`)-[p:`apply`]-(o:`transaction`) where o.status='OVER_DUE' and s.personID= '{}' return o.status".format(
                value)).data())
        clearNum = len(self.graph.run(
            "match (s:`person`)-[p:`apply`]-(o:`transaction`) where o.status='CLEAR' and s.personID= '{}' return o.status".format(
                value)).data())
        rejectedNum = len(self.graph.run(
            "match (s:`person`)-[p:`apply`]-(o:`transaction`) where o.status='REJECT' and s.personID= '{}' return o.status".format(
                value)).data())
        return defaultNum, clearNum, rejectedNum

    '''
        导出统计数据
        Input: path（output route）
        Output: void(output csv)
    '''
    def genData(self,path):
        userID = []
        userFlag = []
        userStatus = []
        # 所有用户状态
        userEntity = self.graph.run("match (s:`person`)  return s.personID,s.flag,s.status").data()
        for i in userEntity:
            userID.append(i['s.personID'])
            userFlag.append(i['s.flag'])
            userStatus.append(i['s.status'])

        n1Num = []
        n2Num = []
        phoneNum = []
        rmtNum = []
        txdNum = []
        txcNum = []
        txrNum = []
        for value in userID:
            n1Num.append(self.network1FlagNum(value=value))
            n2Num.append(self.network2FlagNum(value=value))
            phoneNum.append(self.userPhoneNum(value=value))
            rmtNum.append(self.userRmtNum(value=value))
            c1, c2, c3 = self.userTxConditionNum(value=value)
            txdNum.append(c1)
            txcNum.append(c2)
            txrNum.append(c3)

        df = pd.DataFrame()
        df['userID'] = userID
        df['是否在黑名单'] = userFlag
        df['历史用户评分'] = userStatus
        df['一层关系黑户数'] = n1Num
        df['二层关系黑户数'] = n2Num
        df['持有手机数'] = phoneNum
        df['大额转账数'] = rmtNum
        df['违约贷款数'] = txdNum
        df['完结贷款数'] = txcNum
        df['被拒贷款数'] = txrNum

        # df.to_csv(r'out/graphOut.csv', encoding='utf-8', index=False)
        df.to_csv(path, encoding='utf-8', index=False)