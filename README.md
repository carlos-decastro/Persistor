# 📦 Persistor

Persistor é uma ferramenta robusta de backup para PostgreSQL projetada para funcionar em ambientes onde o usuário possui apenas permissões de `SELECT`. Ela gera um único arquivo `.sql` autocontido e executável que recria toda a estrutura do banco e insere os dados.

## ✨ Diferenciais

- 🛡️ **Permissões Mínimas**: Funciona apenas com `SELECT` nas tabelas e acesso ao `pg_catalog`.
- 🚀 **Performance**: Processamento via streaming com `pg-cursor` (não carrega tabelas inteiras na memória).
- 🧬 **Mapeamento Inteligente**: Reconhece tabelas, colunas, tipos (JSONB, UUID, Arrays, etc.), índices, sequences e constraints.
- 🔗 **Integridade**: Gerencia a ordem correta de criação e utiliza `session_replication_role = replica` para garantir que as constraints não bloqueiem a inserção de dados.
- 📜 **Autocontido**: O arquivo final inclui DDL, Dados e restauração de Sequences.

## 🛠️ Instalação

```bash
npm install
npm run build
```

## 🚀 Como Usar

A ferramenta possui dois comandos principais: `backup` e `compare`.

### 1. Backup completo

Gera um arquivo SQL com estrutura e dados.

```bash
npm start -- backup -d <banco> -u <usuario> -p <porta> -h <host> -P <senha>
```

**Opções de Backup:**
- `-d, --database <string>`: Nome do banco de dados (Obrigatório)
- `-u, --user <string>`: Usuário (Obrigatório)
- `-h, --host <string>`: Host do banco (Padrão: localhost)
- `-p, --port <number>`: Porta (Padrão: 5432)
- `-P, --password <string>`: Senha do banco
- `-s, --schema <string>`: Schema específico (Padrão: public)
- `-t, --tables <string>`: Lista de tabelas separadas por vírgula (Opcional)
- `-o, --output-dir <string>`: Diretório para o arquivo de backup (Padrão: diretório atual)

### 2. Comparação de Schemas

Compara a estrutura de dois bancos e lista o que existe no Banco 1 que não existe (ou é diferente) no Banco 2.

```bash
npm start -- compare --s-db <banco1> --s-user <user1> --s-pass <pass1> --t-db <banco2> --t-user <user2> --t-pass <pass2>
```

**Opções de Comparação (Source):**
- `--s-db <string>`: Banco de origem (Obrigatório)
- `--s-user <string>`: Usuário de origem (Obrigatório)
- `--s-host <string>`: Host de origem (Padrão: localhost)
- `--s-pass <string>`: Senha de origem
- `--s-port <number>`: Porta de origem (Padrão: 5432)
- `--s-schema <string>`: Schema de origem (Padrão: public)

**Opções de Comparação (Target):**
- `--t-db <string>`: Banco de destino (Obrigatário)
- `--t-user <string>`: Usuário de destino (Obrigatório)
- `--t-host <string>`: Host de destino (Padrão: localhost)
- `--t-pass <string>`: Senha de destino
- `--t-port <number>`: Porta de destino (Padrão: 5432)
- `--t-schema <string>`: Schema de destino (Padrão: public)

## 📁 Estrutura do Projeto

- `src/db`: Camada de conexão.
- `src/inspector`: Analisador de schema do PostgreSQL.
- `src/generator`: Gerador de comandos DDL.
- `src/extractor`: Extrator de dados em streaming.
- `src/writer`: Escrita progressiva em arquivo SQL.
- `src/core`: Orquestração do processo completo.

## ⚠️ Requisitos

- Node.js 18+
- PostgreSQL 13+
