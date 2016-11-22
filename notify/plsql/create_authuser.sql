CREATE TABLE IF NOT EXISTS authuser(
  id integer primary key autoincrement, 
  uname char(60) not null, 
  email text, 
  phone char(20) not null, 
  verified int,
  UNIQUE (uname, phone, verified)
);

CREATE TABLE IF NOT EXISTS authy(
  id integer primary key autoincrement, 
  authy_key char(100),
  post_url text,
  get_url text,
  dash_url text,
  UNIQUE (authy_key)
);

CREATE TABLE IF NOT EXISTS twilio(
    id integer primary key autoincrement,
    account_sid char(100),
    auth_token char(100),
    from_number char(20),
    dash_url text,
    UNIQUE(account_sid, auth_token)
);

INSERT OR IGNORE INTO twilio (account_sid, auth_token, from_number, dash_url) VALUES 
  ( 'AC8a74ca68ff5b4707bda10ae83bf80478', 
    'b7b41fa7683022a916c54361cf1fc191', 
    '+18037537244',
    'https://www.twilio.com/console'
);

INSERT OR IGNORE INTO authy (authy_key, post_url, get_url, dash_url) VALUES
  ( 'heJm3VjWY59p3Ei5ohsS5oT2A8LmFrmR', 
    'https://api.authy.com/protected/json/phones/verification/start?',
    'https://api.authy.com/protected/json/phones/verification/check?',
    'https://dashboard.authy.com/applications/43231'
);
