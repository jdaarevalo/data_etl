-- Create table to ingest data
CREATE TABLE IF NOT EXISTS data_challenge (
  created_utc TIMESTAMP not null,
  score int,
  domain varchar(250),
  id varchar(250),
  title varchar(250),
  ups int,
  downs int, 
  num_comments int,
  permalink varchar(250),
  selftext varchar(500),
  link_flair_text varchar(500),
  over_18 bool,
  thumbnail varchar(500),
  subreddit_id varchar(250),
  edited bool,
  link_flair_css_class varchar(250),
  author_flair_css_class varchar(250),
  is_self bool,
  name varchar(250),
  url varchar(250)
);

CREATE INDEX idx_data_challenge_created_utc ON public.data_challenge USING btree (created_utc);

-- Create table posts_2013
CREATE TABLE IF NOT EXISTS public."posts_2013" (
  created_utc timestamp NOT NULL,
  score int4 NULL,
  ups int4 NULL,
  downs int4 NULL,
  permalink text NULL,
  id varchar NULL,
  subreddit_id varchar NULL,
  CONSTRAINT "posts_2013_id_key" UNIQUE (id)
);
CREATE INDEX "idx_posts_2013_created_utc" ON public."posts_2013" USING btree (created_utc);
CREATE INDEX "idx_posts_2013_id" ON public."posts_2013" USING btree (id);

