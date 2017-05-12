# -*- coding: utf-8 -*-
import codecs

import fdb as kinterbasdb
import requests
from bs4 import BeautifulSoup


# $ pip install fdb
# use this documentation for the moment http://www.firebirdsql.org/file/documentation/drivers_documentation/python/3.3.0/tutorial.html#connecting-to-a-database


class tcm_ba(object):

	global cpf
	global mes
	global ano

	def __init__(self, url):
		MAX_RETRIES = 60

		self.session = requests.Session()
		adapter = requests.adapters.HTTPAdapter(max_retries=MAX_RETRIES)
		self.session.mount('https://', adapter)
		self.session.mount('http://', adapter)

		self.headers = {
		    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1',

		}
		self.set_url(url)
		return


	def set_url(self, url):
		self.url = url
		return


	def conectar(self):
		try:
			self.con = kinterbasdb.connect(dsn='D:\minerar_tcm/DADOS.fdb', user='sysdba', password='masterkey', charset='UTF8')
			self.cur = self.con.cursor()
		except:
			print("I am unable to connect to the database")


	def buscar(self):
		payload = {
			'cpf': self.cpf,
			'ano': self.ano,
			'mes': self.mes,
			'pesquisar': 'Pesquisar'
		}
		try:
			r = self.session.post(self.url, data=payload)
		except:
			self.minerar()

		if r.status_code == 200 :
			s = BeautifulSoup(r.text, 'html.parser')
			for tag in s.find_all(class_="table table-striped"):
				if tag.get('id') == "tabelaResultado":
					cells = tag.findAll("td")
					dict = (
							self.cpf,
							self.ano,
							self.mes,
							cells[0].find(text=True),
							cells[1].find(text=True),
							cells[2].find(text=True),
							cells[3].find(text=True),
							cells[4].find(text=True),
							cells[5].find(text=True),
							cells[6].find(text=True),
							cells[7].find(text=True).replace('R$','').replace('.','').replace(',','.'),

					)
					try:
						print('CPF %s consta no mes %s do ano %s ' % (self.cpf, self.mes, self.ano))
						self.cur.execute("INSERT INTO resultado (cpf, ano, mes, matricula, competencia, municipio, unidade, cargo, tipo_servidor, folha, salario_liquido) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" % dict)

					except Exception as e:
						print('Error >> Ao salvar cpf %s , mes %s , ano %s ' % (self.cpf, self.mes, self.ano))
						print(e)
						return False
			return True
		else :
			print('Error >> Falhar ao carregar página')
			print('Error >> Status página --> %d' % r.status_code)
			self.minerar()


	def minerar(self):
		self.conectar()
		self.cur.execute("SELECT cpf, nome FROM pessoas WHERE feito = 'N' ")
		for (documento, nome) in self.cur.fetchall():
			self.cpf = documento
			self.ano = 2017
			for m in (1,2,3):
				self.mes = m
				ret = False
				ret = self.buscar()
				if ret == True:
					self.cur.execute("UPDATE pessoas SET feito = 'S' WHERE cpf = '%s'" % self.cpf)
					self.con.commit()
		self.con.close()


	def __del__(self):
		print('Fechando a conexão...')
		self.con.close()
		print('Conexão finalizada...')


def main():		
	tcm = tcm_ba("http://www.tcm.ba.gov.br/portal-cidadania-municipio/pessoal-por-cpf/")
	tcm.minerar()


if __name__ == "__main__":
	main()		





