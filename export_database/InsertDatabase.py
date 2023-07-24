from database.Database import *
from importing.ImportCsv import *

class InsertDatabase:
    def __init__(self):
        pass

    def insert_into_database(self, csv_path):
        try:
            start_time = time.time()

            con = Database('system', '1993', 'localhost:1521')
            importing_file = ImportCsv()
            data_frame = importing_file.import_csv(csv_path)

            query = """insert into ta_preco_medicamento 
                               (
                               substancia, cnpj, laboratorio, codigo_ggrem, registro,
                                ean_1, ean_2, ean_3, produto, apresentacao, classe_terapeutica, 
                                tipo_de_produto_status, regime_de_preco, pf_sem_imposto, pf_0, 
                                pf_12, pf_17, pf_17_alc, pf_17_5, pf_17_5_alc, pf_18, pf_18_alc, 
                                pf_20, pmc_0, pmc_12, pmc_17, pmc_17_alc, pmc_17_5, pmc_17_5_alc, 
                                pmc_18, pmc_18_alc, pmc_20, restricao_hospitalar, cap, confaz_87, 
                                icms_0, analise_recursal, lista_de_concessa_de_credito_tributario, 
                                comercializado_2020, tarja
                                ) 
                           values 
                               (
                               :1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, to_number(:14), 
                               to_number(:15), to_number(:16), to_number(:17), to_number(:18), to_number(:19), 
                               to_number(:20), to_number(:21), to_number(:22), to_number(:23), to_number(:24), 
                               to_number(:25), to_number(:26), to_number(:27), to_number(:28), to_number(:29), 
                               to_number(:30), to_number(:31), to_number(:32), :33, :34, :35, :36, :37, :38, 
                               :39, :40
                               )"""

            con.execute_query(query, data_frame)
        except Exception as e:
            err, = e.args
            print("Erro: ", err.code)
            print("Mensagem: ", err.message)
        finally:
            print("Inserting time:")
            print("--- %s seconds ---" % (time.time() - start_time))

