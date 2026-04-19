# ir-pa1-crawler

Crawler multithread em Python 3.14 para o PA1 de Recuperação de Informação.

## Requisitos

- Python 3.14
- Ambiente virtual
- Dependências do `requirements.txt`

## Instalação

```bash
python3 -m venv pa1
source pa1/bin/activate
pip3 install -r requirements.txt
```

## Execução

Comando mínimo exigido pela especificação:

```bash
python3 crawler.py -s <SEEDS> -n <LIMIT> [-d]
```

Exemplo:

```bash
python3 crawler.py -s seeds/seeds-teste.txt -n 100 -d
```

## Argumentos extras disponíveis

- `--threads`: quantidade de threads de crawling (padrão: 16)
- `--output-dir`: diretório de saída (padrão: `output`)
- `--timeout`: timeout HTTP em segundos (padrão: 10)
- `--user-agent`: user-agent enviado nas requisições
- `--max-retries`: número de tentativas por requisição (padrão: 2)
- `--max-depth`: profundidade máxima de crawling (opcional)
- `--warc-prefix`: prefixo dos arquivos WARC (padrão: `corpus`)
- `--respect-nofollow`: respeita `rel="nofollow"` em links (opcional)

## Saídas

- `output/warc/`: arquivos `.warc.gz`
- `output/stats/crawl_stats.json`: estatísticas agregadas
- `output/logs/`: logs opcionais

## Observações

- O crawler segue apenas páginas HTML.
- O crawler não revisita URLs normalizadas já vistas.
- O crawler respeita `robots.txt` e atraso mínimo de 100 ms por host quando não há `crawl-delay` explícito.
- Cada arquivo WARC armazena até 1000 páginas.
