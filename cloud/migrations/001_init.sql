-- EEG 心流实验平台 · 建表语句
-- MySQL 5.5 兼容：JSON 字段改用 TEXT

CREATE TABLE IF NOT EXISTS subjects (
  id          INT AUTO_INCREMENT PRIMARY KEY,
  name        VARCHAR(32) NOT NULL,
  age         TINYINT,
  gender      CHAR(1),
  notes       TEXT,
  created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS experiment_templates (
  id          INT AUTO_INCREMENT PRIMARY KEY,
  name        VARCHAR(64) NOT NULL,
  group_type  VARCHAR(16) NOT NULL,   -- 'control' | 'experiment'
  switch_type VARCHAR(16) NOT NULL,   -- 'none' | 'math_lang' | 'lang_art' | 'math_art'
  phases_json TEXT NOT NULL,           -- JSON 字符串
  created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS sessions (
  id              VARCHAR(36) PRIMARY KEY,
  subject_id      INT NOT NULL,
  template_id     INT NOT NULL,
  operator_name   VARCHAR(32),
  status          VARCHAR(16) DEFAULT 'pending',  -- pending | running | paused | completed
  started_at      TIMESTAMP NULL,
  ended_at        TIMESTAMP NULL,
  notes           TEXT,
  created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (subject_id) REFERENCES subjects(id),
  FOREIGN KEY (template_id) REFERENCES experiment_templates(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS fss_results (
  id          INT AUTO_INCREMENT PRIMARY KEY,
  session_id  VARCHAR(36) NOT NULL,
  round       TINYINT NOT NULL,
  phase       VARCHAR(16) NOT NULL,
  answers     TEXT NOT NULL,            -- JSON 字符串 [7] int
  submitted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (session_id) REFERENCES sessions(id),
  INDEX idx_fss_session (session_id, round)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS markers (
  id          INT AUTO_INCREMENT PRIMARY KEY,
  session_id  VARCHAR(36) NOT NULL,
  code        TINYINT NOT NULL,
  source      VARCHAR(16) NOT NULL,     -- 'operator' | 'subject' | 'auto'
  label       VARCHAR(64),
  phase       VARCHAR(16),
  ts          BIGINT NOT NULL,
  created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (session_id) REFERENCES sessions(id),
  INDEX idx_session_ts (session_id, ts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS metrics_snapshots (
  id                  BIGINT AUTO_INCREMENT PRIMARY KEY,
  session_id          VARCHAR(36) NOT NULL,
  ts                  BIGINT NOT NULL,
  phase_index         TINYINT NOT NULL,
  phase_id            VARCHAR(16) NOT NULL,
  delta               FLOAT,
  theta               FLOAT,
  alpha               FLOAT,
  beta                FLOAT,
  gamma               FLOAT,
  theta_alpha_ratio   FLOAT,
  spectral_entropy    FLOAT,
  cognitive_load_index FLOAT,
  sq_ch1              FLOAT,
  sq_ch2              FLOAT,
  sq_ch3              FLOAT,
  sq_ch4              FLOAT,
  created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (session_id) REFERENCES sessions(id),
  INDEX idx_session_phase (session_id, phase_index),
  INDEX idx_ts (ts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS baselines (
  id          INT AUTO_INCREMENT PRIMARY KEY,
  session_id  VARCHAR(36) NOT NULL,
  phase_id    VARCHAR(16) NOT NULL,
  ratio_mean  FLOAT,
  ratio_std   FLOAT,
  alpha_mean  FLOAT,
  alpha_std   FLOAT,
  beta_mean   FLOAT,
  beta_std    FLOAT,
  samples     INT,
  created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (session_id) REFERENCES sessions(id),
  INDEX idx_baselines_session (session_id, phase_id, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS timer_state (
  session_id     VARCHAR(36) PRIMARY KEY,
  phase_index    TINYINT NOT NULL DEFAULT 0,
  time_left      INT NOT NULL DEFAULT 0,
  time_in_phase  INT NOT NULL DEFAULT 0,
  running        TINYINT NOT NULL DEFAULT 0,
  auto_mode      TINYINT NOT NULL DEFAULT 1,
  template_type  VARCHAR(16) NOT NULL DEFAULT 'control',
  updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  FOREIGN KEY (session_id) REFERENCES sessions(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
