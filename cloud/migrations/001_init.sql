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
