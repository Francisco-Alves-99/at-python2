from bs4 import BeautifulSoup
from selenium import webdriver
import time
import re
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import IntegrityError
import sqlite3

# 1.
navegador = webdriver.Chrome()

url = "https://www.imdb.com/chart/top/"

navegador.get(url)
time.sleep(5)

html = navegador.page_source

navegador.quit()



soup = BeautifulSoup(html, 'html.parser')

filmes_lista = soup.find_all('li', class_='ipc-metadata-list-summary-item')

titulos, anos, notas = [], [], []

for filme in filmes_lista:

    titulo_com_numero = filme.find('h3').text.strip()
    titulo = re.sub(r'^\d+\.\s*', '', titulo_com_numero)
    titulos.append(titulo)

    spans = filme.find_all('span', class_='sc-e8bccfea-7 hvVhYi cli-title-metadata-item')
    ano = spans[0].text.strip()
    anos.append(ano)

    nota = filme.find('span', class_='ipc-rating-star--rating').text.strip()
    nota = nota.replace(',', '.')
    notas.append(float(nota))


df_filmes = pd.DataFrame({'Título': titulos, 'Ano de lançamento': anos, 'Nota': notas})

for i in range(10):
    print(df_filmes['Título'][i])

# 2.

for i in range(5):
    print(f"{df_filmes['Título'][i]} ({df_filmes['Ano de lançamento'][i]}) - Nota: {df_filmes['Nota'][i]}")

# 3.
Base = declarative_base()

# Classe base TV
class TV:
    def __init__(self, title, year):
        self.title = title
        self.year = year

    def __str__(self):
        return f"{self.title} ({self.year})"

# Classe Movie que herda de TV
class Movie(TV, Base):
    __tablename__ = 'movies'
    
    id = Column(Integer, primary_key=True)
    title = Column(String, nullable=False)
    year = Column(Integer, nullable=False)
    rating = Column(Float, nullable=False)

    def __init__(self, title, year, rating):
        super().__init__(title, year)  # Chama o construtor da classe base
        self.rating = rating

    def __str__(self):
        return f"{self.title} ({self.year}) - Nota: {self.rating}"

# Classe Series que herda de TV
class Series(TV, Base):
    __tablename__ = 'series'
    
    id = Column(Integer, primary_key=True)
    title = Column(String, nullable=False)
    year = Column(Integer, nullable=False)
    seasons = Column(Integer, nullable=False)
    episodes = Column(Integer, nullable=False)

    def __init__(self, title, year, seasons, episodes):
        super().__init__(title, year)  # Chama o construtor da classe base
        self.seasons = seasons
        self.episodes = episodes

    def __str__(self):
        return f"{self.title} ({self.year}) - {self.seasons} temporadas, {self.episodes} episódios"



# 4.
lista_objetos_filmes = []
lista_objetos_series = []
for index, linha in df_filmes.iterrows():
    filme = Movie(linha['Título'], linha['Ano de lançamento'], linha['Nota'])
    lista_objetos_filmes.append(filme)

serie1 = Series("Breaking Bad", 2008, 5, 62)    
serie2 = Series("Sherlock", 2010, 4, 15)
lista_objetos_series.append(serie1)    
lista_objetos_series.append(serie2)    

for filme in lista_objetos_filmes:
    print(filme)
print("----------------------------------------")
for serie in lista_objetos_series:
    print(serie)       


# 5.

# Criando a conexão com o banco de dados SQLite
engine = create_engine('sqlite:///imdb.db', echo=True)

# Criando as tabelas no banco de dados
Base.metadata.create_all(engine)

# Criando a sessão para inserir dados no banco
Session = sessionmaker(bind=engine)
session = Session()

# Função para adicionar um filme ao banco de dados com verificação de duplicação
def adicionar_filme(title, year, rating):
    try:
        # Verificar se o filme já existe no banco de dados
        filme_existente = session.query(Movie).filter_by(title=title, year=year).first()
        if filme_existente:
            print(f"Filme '{title}' ({year}) já existe no banco de dados.")
            return  # Se o filme já existir, não adiciona novamente

        # Caso não exista, cria e adiciona o novo filme
        novo_filme = Movie(title=title, year=year, rating=rating)
        session.add(novo_filme)
        session.commit()
        print(f"Filme '{title}' ({year}) adicionado com sucesso.")
    
    except IntegrityError as e:
        # Caso ocorra erro de integridade (por exemplo, chave duplicada)
        print(f"Erro de integridade ao adicionar filme: {e}")
        session.rollback()  # Faz rollback caso ocorra erro
    
    except Exception as e:
        # Qualquer outro erro inesperado
        print(f"Ocorreu um erro ao adicionar o filme: {e}")
        session.rollback()

# Função para adicionar uma série ao banco de dados com verificação de duplicação
def adicionar_serie(title, year, seasons, episodes):
    try:
        # Verificar se a série já existe no banco de dados
        serie_existente = session.query(Series).filter_by(title=title, year=year).first()
        if serie_existente:
            print(f"Série '{title}' ({year}) já existe no banco de dados.")
            return  # Se a série já existir, não adiciona novamente

        # Caso não exista, cria e adiciona a nova série
        nova_serie = Series(title=title, year=year, seasons=seasons, episodes=episodes)
        session.add(nova_serie)
        session.commit()
        print(f"Série '{title}' ({year}) adicionada com sucesso.")
    
    except IntegrityError as e:
        # Caso ocorra erro de integridade (por exemplo, chave duplicada)
        print(f"Erro de integridade ao adicionar série: {e}")
        session.rollback()  # Faz rollback caso ocorra erro
    
    except Exception as e:
        # Qualquer outro erro inesperado
        print(f"Ocorreu um erro ao adicionar a série: {e}")
        session.rollback()


for filme in lista_objetos_filmes:
    adicionar_filme(filme.title, filme.year, filme.rating)

for serie in lista_objetos_series:
    adicionar_serie(serie.title, serie.year, serie.seasons, serie.episodes)
      

# 6.

try:
    
    conn = sqlite3.connect('imdb.db') 

    df_movies = pd.read_sql("SELECT * FROM movies", conn)
    df_series = pd.read_sql("SELECT * FROM series", conn)

    print("Movies:")
    print(df_movies.head(5))

    print("\nSeries:")
    print(df_series.head(5))

except sqlite3.Error as e:
    print(f"Erro ao conectar ao banco de dados ou executar as consultas: {e}")



# 7.

df_movies_ordenado = df_movies.sort_values(by='rating', ascending=False)
print("Filmes ordenados pelo rating (maior para menor):")
print(df_movies_ordenado)

df_movies_filtrado = df_movies[df_movies['rating'] > 9.0]
print("\nFilmes com rating acima de 9.0:")
print(df_movies_filtrado)

print("\nTop 5 filmes mais bem avaliados:")
print(df_movies_ordenado.head(5))

# 8.
try:
    df_movies.to_csv('movies.csv', index=False)
    print("Arquivo movies.csv criado com sucesso")

    df_series.to_csv('series.csv', index=False)
    print("Arquivo series.csv criado com sucesso")

    df_movies.to_json('movies.json', orient='records', indent=4, force_ascii=False)
    print("Arquivo movies.json criado com sucesso")

    df_series.to_json('series.json', orient='records', indent=4, force_ascii=False)
    print("Arquivo series.json criado com sucesso")

except Exception as e:
    print(f"Ocorreu um erro ao tentar salvar os arquivos: {e}")

