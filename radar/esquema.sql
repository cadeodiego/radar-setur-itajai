-- RADAR — esquema do monitoramento de mídia
-- Escrito num subconjunto portável: roda em SQLite (protótipo local) e MySQL
-- (servidor) sem reescrita. Os marcadores {{...}} são trocados por dialeto em
-- banco.py. ENUM virou VARCHAR+CHECK e JSON virou TEXT de propósito — é o que
-- torna o mesmo arquivo válido nos dois.
--
-- Convenções:
--   · natureza (medido|declarado|estimado) acompanha TODO número que veio de
--     fora. É o contrato herdado de eventos-itajai/imprensa/recortes.json.
--   · rd_avaliacao é append-only. Reavaliar cria linha; nunca sobrescreve.
--   · rd_edicao congela o que foi publicado. Edição é documento, não view.

-- ---------------------------------------------------------------- versão
CREATE TABLE IF NOT EXISTS rd_schema_versao (
  versao      INTEGER NOT NULL,
  aplicado_em TEXT    NOT NULL,
  nota        TEXT
){{ENGINE}};

-- ------------------------------------------------------- config e vocabulário
CREATE TABLE IF NOT EXISTS rd_config (
  chave TEXT NOT NULL PRIMARY KEY,
  valor TEXT
){{ENGINE}};

CREATE TABLE IF NOT EXISTS rd_veiculo (
  id               {{PK}},
  dominio          VARCHAR(190) NOT NULL,
  nome             VARCHAR(190) NOT NULL,
  tipo             VARCHAR(20)  NOT NULL DEFAULT 'portal'
                   CHECK (tipo IN ('portal','jornal','tv','radio','blog','revista','rede','agregador','oficial','ia')),
  uf               VARCHAR(2),
  cidade           VARCHAR(120),
  -- alcance sem natureza declarada não é alcance, é boato com número
  alcance_mensal   BIGINT,
  alcance_fonte    VARCHAR(190),
  alcance_natureza VARCHAR(10) CHECK (alcance_natureza IN ('medido','declarado','estimado')),
  peso             REAL NOT NULL DEFAULT 1.0,
  ativo            INTEGER NOT NULL DEFAULT 1
){{ENGINE}};
CREATE UNIQUE INDEX IF NOT EXISTS uk_veiculo_dominio ON rd_veiculo (dominio);

CREATE TABLE IF NOT EXISTS rd_eixo (
  id       {{PK}},
  slug     VARCHAR(40)  NOT NULL,
  nome     VARCHAR(120) NOT NULL,
  cor_claro  VARCHAR(9),
  cor_escuro VARCHAR(9),
  sensivel INTEGER NOT NULL DEFAULT 0,
  ordem    INTEGER NOT NULL DEFAULT 0,
  ativo    INTEGER NOT NULL DEFAULT 1
){{ENGINE}};
CREATE UNIQUE INDEX IF NOT EXISTS uk_eixo_slug ON rd_eixo (slug);

-- vigência existe porque o edital cobra "termos rastreados" DO PERÍODO:
-- o relatório de fevereiro não pode alegar que rastreava um termo criado em março
CREATE TABLE IF NOT EXISTS rd_termo (
  id          {{PK}},
  termo       VARCHAR(190) NOT NULL,
  tipo        VARCHAR(20)  NOT NULL DEFAULT 'tema'
              CHECK (tipo IN ('marca','evento','pessoa','concorrente','tema','negativo')),
  variantes   TEXT,
  obrigatorio INTEGER NOT NULL DEFAULT 0,
  vigente_de  TEXT NOT NULL,
  vigente_ate TEXT
){{ENGINE}};

CREATE TABLE IF NOT EXISTS rd_consulta (
  id        {{PK}},
  coletor   VARCHAR(40)  NOT NULL,
  eixo_id   INTEGER,
  expressao VARCHAR(500) NOT NULL,
  params    TEXT,
  ativa     INTEGER NOT NULL DEFAULT 1,
  ultima_em TEXT,
  cursor    VARCHAR(190)
){{ENGINE}};

-- log de proveniência: é a "verificação da origem dos dados" que o edital exige
CREATE TABLE IF NOT EXISTS rd_coleta (
  id             {{PK}},
  coletor        VARCHAR(40) NOT NULL,
  consulta_id    INTEGER,
  params         TEXT,
  iniciado_em    TEXT NOT NULL,
  terminado_em   TEXT,
  status         VARCHAR(12) NOT NULL DEFAULT 'rodando'
                 CHECK (status IN ('rodando','ok','falha','parcial')),
  achados        INTEGER NOT NULL DEFAULT 0,
  novos          INTEGER NOT NULL DEFAULT 0,
  custo_unidades REAL NOT NULL DEFAULT 0,
  erro           TEXT
){{ENGINE}};

-- ------------------------------------------------------------ item e cluster
-- rd_item é a INSERÇÃO. rd_cluster é a MATÉRIA. A mesma matéria republicada em
-- 5 portais são 5 inserções e 1 matéria — o edital cobra inserção ("≥10 por
-- evento"); contar só uma das duas infla ou subnotifica.
CREATE TABLE IF NOT EXISTS rd_cluster (
  id               {{PK}},
  titulo_canonico  VARCHAR(500) NOT NULL,
  primeira_pub     TEXT,
  ultima_pub       TEXT,
  n_insercoes      INTEGER NOT NULL DEFAULT 0,
  eixo_principal_id INTEGER,
  alcance_somado   BIGINT,
  nota_media       REAL
){{ENGINE}};

CREATE TABLE IF NOT EXISTS rd_item (
  id             {{PK}},
  cliente_id     INTEGER NOT NULL DEFAULT 1,
  canal          VARCHAR(20) NOT NULL DEFAULT 'imprensa'
                 CHECK (canal IN ('imprensa','instagram','facebook','youtube','tiktok','x','tv','radio','podcast','ia','manual')),
  coletor        VARCHAR(40),
  coleta_id      INTEGER,

  url            TEXT NOT NULL,
  url_canonica   VARCHAR(700) NOT NULL,
  dedupe_hash    VARCHAR(40)  NOT NULL,

  titulo         VARCHAR(500) NOT NULL,
  titulo_simhash TEXT,               -- 64 bits como string decimal: portável
  titulo_bucket  INTEGER,            -- 16 bits altos do simhash, pré-filtro indexável
  resumo         TEXT,
  corpo          TEXT,
  autor          VARCHAR(190),

  veiculo_id     INTEGER,
  publicado_em   TEXT,
  -- o "~data (aprox.)" do Diarinho vira estado consultável, não gambiarra no texto
  publicado_precisao VARCHAR(12) NOT NULL DEFAULT 'desconhecida'
                 CHECK (publicado_precisao IN ('exata','aproximada','desconhecida')),

  capa_url       TEXT,
  capa_hash      VARCHAR(40),
  capa_status    VARCHAR(10) NOT NULL DEFAULT 'pendente'
                 CHECK (capa_status IN ('pendente','ok','ausente','falha','gerada')),

  cluster_id     INTEGER,
  relevancia     REAL NOT NULL DEFAULT 1.0,
  estado         VARCHAR(12) NOT NULL DEFAULT 'bruto'
                 CHECK (estado IN ('bruto','enriquecido','classificado','publicado','descartado')),
  motivo_descarte VARCHAR(120),
  avaliacao_id   INTEGER,            -- aponta para a avaliação VIGENTE

  bruto          TEXT,               -- payload original do feed/API, intocado
  criado_em      TEXT NOT NULL,
  atualizado_em  TEXT NOT NULL
){{ENGINE}};
CREATE UNIQUE INDEX IF NOT EXISTS uk_item_dedupe  ON rd_item (canal, dedupe_hash);
CREATE INDEX IF NOT EXISTS ix_item_pub     ON rd_item (publicado_em);
CREATE INDEX IF NOT EXISTS ix_item_estado  ON rd_item (estado, publicado_em);
CREATE INDEX IF NOT EXISTS ix_item_bucket  ON rd_item (titulo_bucket, publicado_em);
CREATE INDEX IF NOT EXISTS ix_item_veiculo ON rd_item (veiculo_id, publicado_em);
CREATE INDEX IF NOT EXISTS ix_item_cluster ON rd_item (cluster_id);

CREATE TABLE IF NOT EXISTS rd_item_eixo (
  item_id   INTEGER NOT NULL,
  eixo_id   INTEGER NOT NULL,
  confianca REAL NOT NULL DEFAULT 1.0,
  PRIMARY KEY (item_id, eixo_id)
){{ENGINE}};

CREATE TABLE IF NOT EXISTS rd_item_termo (
  item_id    INTEGER NOT NULL,
  termo_id   INTEGER NOT NULL,
  ocorrencias INTEGER NOT NULL DEFAULT 1,
  PRIMARY KEY (item_id, termo_id)
){{ENGINE}};

-- métrica de rede é SÉRIE TEMPORAL, não campo: um Reels com 2 mil views no dia 1
-- tem 40 mil no dia 7, e o relatório precisa saber quanto tinha quando foi entregue
CREATE TABLE IF NOT EXISTS rd_metrica (
  id               {{PK}},
  item_id          INTEGER NOT NULL,
  medido_em        TEXT NOT NULL,
  views            BIGINT,
  curtidas         BIGINT,
  comentarios      BIGINT,
  compartilhamentos BIGINT,
  salvos           BIGINT,
  engajamento      REAL,
  alcance_estimado BIGINT,
  natureza         VARCHAR(10) CHECK (natureza IN ('medido','declarado','estimado')),
  fonte            VARCHAR(190)
){{ENGINE}};
CREATE UNIQUE INDEX IF NOT EXISTS uk_metrica ON rd_metrica (item_id, medido_em);

-- ------------------------------------------------------------ humor auditável
CREATE TABLE IF NOT EXISTS rd_rubrica (
  id          {{PK}},
  versao      VARCHAR(12)  NOT NULL,
  titulo      VARCHAR(190) NOT NULL,
  texto_md    TEXT NOT NULL,          -- a rubrica integral, como será citada no relatório
  prompt_md   TEXT NOT NULL,          -- o template exato enviado ao modelo
  modelo      VARCHAR(60) NOT NULL,   -- ID COMPLETO E DATADO. Alias tipo "latest" muda
                                      -- a série histórica sem commit — proibido.
  temperatura REAL NOT NULL DEFAULT 0,
  faixas      TEXT NOT NULL,          -- JSON com o mapa nota→polaridade
  hash        VARCHAR(64) NOT NULL,   -- sha256(texto||prompt||modelo||faixas); mudou = rubrica nova
  vigente_de  TEXT NOT NULL,
  vigente_ate TEXT,
  autor       VARCHAR(80)
){{ENGINE}};
CREATE UNIQUE INDEX IF NOT EXISTS uk_rubrica_versao ON rd_rubrica (versao);

CREATE TABLE IF NOT EXISTS rd_avaliacao (
  id            {{PK}},
  item_id       INTEGER NOT NULL,
  rubrica_id    INTEGER NOT NULL,
  nota          INTEGER NOT NULL CHECK (nota BETWEEN 0 AND 10),
  faixa         VARCHAR(20),
  -- o KPI do contrato é o percentual, não a média: a nota é o instrumento
  polaridade    VARCHAR(10) NOT NULL CHECK (polaridade IN ('positivo','neutro','negativo')),
  justificativa VARCHAR(400),
  -- trechos citados LITERALMENTE do texto avaliado; validados por busca no corpo.
  -- Sem trecho literal não é auditável, é opinião de caixa-preta.
  evidencias    TEXT,
  confianca     VARCHAR(10) NOT NULL DEFAULT 'media' CHECK (confianca IN ('alta','media','baixa')),
  modelo        VARCHAR(60),
  tokens_in     INTEGER,
  tokens_out    INTEGER,
  custo_usd     REAL,
  origem        VARCHAR(10) NOT NULL DEFAULT 'auto' CHECK (origem IN ('auto','humano','revisado')),
  revisor       VARCHAR(80),
  nota_anterior INTEGER,
  criado_em     TEXT NOT NULL
){{ENGINE}};
CREATE INDEX IF NOT EXISTS ix_aval_item ON rd_avaliacao (item_id, criado_em);

CREATE TABLE IF NOT EXISTS rd_rubrica_calibragem (
  id         {{PK}},
  rubrica_id INTEGER NOT NULL,
  gold_set   VARCHAR(80),
  n          INTEGER,
  mae        REAL,          -- erro médio em pontos; aceite ≤ 1,0
  acerto_faixa REAL,        -- aceite ≥ 0,80
  kappa      REAL,
  rodado_em  TEXT,
  detalhe    TEXT
){{ENGINE}};

-- ------------------------------------------- a natureza do número (diferencial)
-- Clipping de mercado repete release. Este carimba de onde o número veio.
-- Caso-teste: Marejada 2025 sai como 205 mil num veículo e 260.090 em outro.
CREATE TABLE IF NOT EXISTS rd_numero (
  id                   {{PK}},
  item_id              INTEGER NOT NULL,
  cluster_id           INTEGER,
  indicador            VARCHAR(80) NOT NULL,
  valor                REAL,
  unidade              VARCHAR(20),
  natureza             VARCHAR(10) NOT NULL CHECK (natureza IN ('medido','declarado','estimado')),
  fonte_declarante     VARCHAR(190),
  metodologia_declarada TEXT,
  texto_original       VARCHAR(500),   -- a frase exata da matéria
  confianca            VARCHAR(10) CHECK (confianca IN ('alta','media','baixa')),
  conflito             TEXT,
  verificado_em        TEXT,           -- NULL = o _verificacao PENDENTE do recortes.json
  verificado_por       VARCHAR(80)
){{ENGINE}};
CREATE INDEX IF NOT EXISTS ix_numero_item ON rd_numero (item_id);

-- ---------------------------------------------------------- publicação e prova
-- Congelar é a decisão mais importante do esquema: relatório entregue em março
-- tem que sair idêntico em setembro, mesmo depois de trocar rubrica.
CREATE TABLE IF NOT EXISTS rd_edicao (
  id           {{PK}},
  tipo         VARCHAR(10) NOT NULL CHECK (tipo IN ('diaria','semanal','mensal','evento')),
  periodo_ini  TEXT NOT NULL,
  periodo_fim  TEXT NOT NULL,
  evento       VARCHAR(190),
  rubrica_id   INTEGER,
  termos_snapshot  TEXT,   -- o que era rastreado NO PERÍODO
  fontes_snapshot  TEXT,
  limites_snapshot TEXT,   -- o que NÃO foi coberto; é o que faz o resto valer
  metricas     TEXT,
  corpo_html   TEXT,
  gerado_em    TEXT NOT NULL,
  hash         VARCHAR(64),
  chave        VARCHAR(22)
){{ENGINE}};
CREATE UNIQUE INDEX IF NOT EXISTS uk_edicao ON rd_edicao (tipo, periodo_ini, periodo_fim, evento);
CREATE UNIQUE INDEX IF NOT EXISTS uk_edicao_chave ON rd_edicao (chave);

CREATE TABLE IF NOT EXISTS rd_edicao_item (
  edicao_id          INTEGER NOT NULL,
  item_id            INTEGER NOT NULL,
  nota_congelada     INTEGER,
  polaridade_congelada VARCHAR(10),
  alcance_congelado  BIGINT,
  PRIMARY KEY (edicao_id, item_id)
){{ENGINE}};

-- ------------------------------------------------------------------ operação
CREATE TABLE IF NOT EXISTS rd_alerta (
  id            {{PK}},
  item_id       INTEGER,
  regra         VARCHAR(80) NOT NULL,
  nivel         VARCHAR(10) NOT NULL DEFAULT 'aviso' CHECK (nivel IN ('info','aviso','critico')),
  disparado_em  TEXT NOT NULL,
  enviado_em    TEXT,
  destinatarios TEXT,
  corpo         TEXT
){{ENGINE}};

CREATE TABLE IF NOT EXISTS rd_fila (
  id         {{PK}},
  tarefa     VARCHAR(20) NOT NULL
             CHECK (tarefa IN ('enriquecer','capa','classificar','transcrever','geo','metrica')),
  item_id    INTEGER,
  prioridade INTEGER NOT NULL DEFAULT 5,
  tentativas INTEGER NOT NULL DEFAULT 0,
  proxima_em TEXT,
  erro       TEXT
){{ENGINE}};
CREATE INDEX IF NOT EXISTS ix_fila ON rd_fila (tarefa, proxima_em, prioridade);

CREATE TABLE IF NOT EXISTS rd_estado (
  chave         VARCHAR(120) NOT NULL PRIMARY KEY,
  valor         TEXT,
  cifrado       INTEGER NOT NULL DEFAULT 0,
  atualizado_em TEXT
){{ENGINE}};

CREATE TABLE IF NOT EXISTS rd_capa (
  hash       VARCHAR(40) NOT NULL PRIMARY KEY,
  largura    INTEGER,
  altura     INTEGER,
  bytes      INTEGER,
  mime       VARCHAR(40),
  origem_url TEXT,
  lqip       VARCHAR(600),   -- data-URI de ~24px: o wall pinta borrado antes de baixar
  baixado_em TEXT,
  formato    VARCHAR(6) CHECK (formato IN ('webp','jpeg','png','svg'))
){{ENGINE}};

CREATE TABLE IF NOT EXISTS rd_log (
  id       {{PK}},
  quando   TEXT NOT NULL,
  nivel    VARCHAR(10) NOT NULL DEFAULT 'info',
  origem   VARCHAR(40),
  mensagem TEXT
){{ENGINE}};
