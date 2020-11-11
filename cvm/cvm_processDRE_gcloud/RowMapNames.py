# %%
MapaNiveis = {
    'Receita Líquida' : 1,
    'Custos': 2,
    'Lucro Bruto':3,
    'Despesas Administrativas':4,
        'Despesas com Vendas': 4.1,
        'Despesas Gerais e Administrativas': 4.2,
    'Despesas/Receitas Operacionais':5,
    'Resultado de Equivalência Patrimonial':6,
    'EBIT' : 7,
    'Resultado Operacional':8,
    'Resultado Não Operacional':9,
    'Resultado Financeiro': 10,
        'Receitas Financeiras': 10.1,
        'Despesas Financeiras': 10.2,
    'EBT': 11,
    'Impostos':12,
    'Lucro Líquido': 13,
}
MapNivel2 = {
    #1
    'Receitas da Intermediação Financeira' : 'Receita Líquida', #Bancos (Ex.: ITAU)
    'Receitas das Operações' : 'Receita Líquida', #Seguradoras (Ex.:BBSE)
    'Receita de Venda de Bens e/ou Serviços': 'Receita Líquida', #Geral (Ex.: Vale)
    #2
    'Despesas da Intermediação Financeira': 'Custos', #Bancos (Ex.: ITAU)
    'Sinistros e Despesas das Operações':'Custos', #Seguradoras (Ex.:BBSE)
    'Custo dos Bens e/ou Serviços Vendidos': 'Custos', #Geral (Ex.: Vale)
    #3
    'Resultado Bruto': 'Lucro Bruto', #Geral (Ex.: Vale) e Seguradoras (Ex.:BBSE)
    'Resultado Bruto Intermediação Financeira': 'Lucro Bruto', #Bancos (Ex.: ITAU)
    #4
    'Despesas Administrativas': 'Despesas Administrativas', #Seguradoras (Ex.:BBSE)
    #5
    'Despesas/Receitas Operacionais': 'Despesas/Receitas Operacionais', #Geral (Ex.: Vale)
    'Outras Despesas/Receitas Operacionais': 'Despesas/Receitas Operacionais', #Bancos (Ex.: ITAU)
    'Outras Receitas e Despesas Operacionais': 'Despesas/Receitas Operacionais', #Seguradoras (Ex.:BBSE)
    #6
    'Resultado de Equivalência Patrimonial':'Resultado de Equivalência Patrimonial',#Seguradoras (Ex.:BBSE)
    #7
    'Resultado Antes do Resultado Financeiro e dos Tributos' : 'EBIT', #Geral (Ex.: Vale) e Seguradoras (Ex.:BBSE)
    #8
    'Resultado Operacional': 'Resultado Operacional', #Bancos (Ex.: ITAU) (Individual)
    #9
    'Resultado Não Operacional': 'Resultado Não Operacional', #Bancos (Ex.: ITAU) (Individual)
    #10
    'Resultado Financeiro' : 'Resultado Financeiro', #Geral (Ex.: Vale) e Seguradoras (Ex.:BBSE)
    #11
    'Resultado Antes dos Tributos sobre o Lucro': 'EBT', #Geral (Ex.: Vale) e Seguradoras (Ex.:BBSE) e Bancos (Ex.: ITAU) (Consolidado)
    'Resultado Antes Tributação/Participações':'EBT', #Bancos (Ex.: ITAU) (Individual)
    #12
    'Imposto de Renda e Contribuição Social sobre o Lucro': 'Impostos', ##Geral (Ex.: Vale) e Seguradoras (Ex.:BBSE) e Bancos (Ex.: ITAU) (Consolidado)
    'Provisão para IR e Contribuição Social': 'Impostos', #Bancos (Ex.: ITAU) (Individual)
    #13
    'Lucro/Prejuízo Consolidado do Período' : 'Lucro Líquido', #Geral (Ex.: Vale) e Seguradoras (Ex.:BBSE) e Bancos (Ex.: ITAU) 
    'Lucro/Prejuízo do Período':'Lucro Líquido', #Geral (Ex.: Vale) e Seguradoras (Ex.:BBSE) e Bancos (Ex.: ITAU) 
}
MapNivel3 = {
    'Despesas com Vendas': 'Despesas com Vendas',
    'Despesas Gerais e Administrativas' : 'Despesas Gerais e Administrativas',
    'Receitas Financeiras': 'Receitas Financeiras',
    'Despesas Financeiras': 'Despesas Financeiras',
    #6
    'Despesas de Depreciação e Amortização' : 'Amortização/Depreciação'
}
MapGrupo = {
    'DF Individual - Demonstração do Resultado': 'Individual',
    'DF Consolidado - Demonstração do Resultado': 'Consolidado'
}