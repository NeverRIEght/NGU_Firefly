CREATE TABLE IF NOT EXISTS "file" (
	"id" INTEGER NOT NULL,
	"file_name" TEXT NOT NULL,
	"absolute_path" TEXT NOT NULL,
	"file_size_bytes" INTEGER NOT NULL,
	"sha256_hash" TEXT,
	PRIMARY KEY("id")
);

CREATE TABLE IF NOT EXISTS "display" (
	"id" INTEGER NOT NULL,
	"width_px" INTEGER NOT NULL,
	"height_px" INTEGER NOT NULL,
	"display_aspect_ratio" TEXT NOT NULL,
	"pixel_aspect_ratio" TEXT NOT NULL,
	"pixel_format" TEXT NOT NULL,
	"chroma_sample_location" TEXT NOT NULL,
	PRIMARY KEY("id")
);

CREATE TABLE IF NOT EXISTS "playback" (
	"id" INTEGER NOT NULL,
	"duration_seconds" REAL NOT NULL,
	"frames_counted" INTEGER,
	"avg_frame_rate" TEXT,
	"r_frame_rate" TEXT,
	PRIMARY KEY("id")
);

CREATE TABLE IF NOT EXISTS "encoding" (
	"id" INTEGER NOT NULL,
	"codec" TEXT NOT NULL,
	"preset" TEXT,
	"encoder" TEXT,
	PRIMARY KEY("id")
);

CREATE TABLE IF NOT EXISTS "cpu" (
	"id" INTEGER NOT NULL,
	"cpu_name" TEXT NOT NULL,
	"cpu_threads" INTEGER NOT NULL,
	PRIMARY KEY("id")
);

CREATE TABLE IF NOT EXISTS "environment" (
	"id" INTEGER NOT NULL,
	"firefly_version" TEXT NOT NULL,
	"ffmpeg_version" TEXT NOT NULL,
	"compression_engine_version" INTEGER NOT NULL,
	PRIMARY KEY("id")
);

CREATE TABLE IF NOT EXISTS "evaluation_metrics" (
	"id" INTEGER NOT NULL,
	"name" TEXT NOT NULL,
	"version" TEXT,
	PRIMARY KEY("id")
);

CREATE TABLE IF NOT EXISTS "job_stages" (
	"id" INTEGER NOT NULL,
	"name" TEXT NOT NULL,
	PRIMARY KEY("id")
);

CREATE TABLE IF NOT EXISTS "segment_statuses" (
	"id" INTEGER NOT NULL,
	"name" TEXT NOT NULL,
	PRIMARY KEY("id")
);

CREATE TABLE IF NOT EXISTS "iteration_stages" (
	"id" INTEGER NOT NULL,
	"name" TEXT NOT NULL,
	PRIMARY KEY("id")
);

CREATE TABLE IF NOT EXISTS "schema" (
	"version" INTEGER NOT NULL
    PRIMARY KEY("version")
);

CREATE TABLE IF NOT EXISTS "color" (
	"id" INTEGER NOT NULL,
	"hdr_format_id" INTEGER,
	"color_primaries_id" INTEGER,
	"color_trc_id" INTEGER,
	"colorspace_id" INTEGER,
	"color_range_id" INTEGER,
	"max_cll" TEXT,
	"master_display" TEXT,
	"dovi_profile" TEXT,
	PRIMARY KEY("id")
);

CREATE TABLE IF NOT EXISTS "hdr_format" (
	"id" INTEGER NOT NULL,
	"name" TEXT NOT NULL,
	PRIMARY KEY("id"),
	FOREIGN KEY ("id") REFERENCES "color"("hdr_format_id")
	ON UPDATE NO ACTION ON DELETE NO ACTION
);

CREATE TABLE IF NOT EXISTS "color_standards" (
	"id" INTEGER NOT NULL,
	"name" TEXT NOT NULL,
	PRIMARY KEY("id"),
	FOREIGN KEY ("id") REFERENCES "color"("color_primaries_id")
	ON UPDATE NO ACTION ON DELETE NO ACTION,
	FOREIGN KEY ("id") REFERENCES "color"("color_trc_id")
	ON UPDATE NO ACTION ON DELETE NO ACTION,
	FOREIGN KEY ("id") REFERENCES "color"("colorspace_id")
	ON UPDATE NO ACTION ON DELETE NO ACTION
);

CREATE TABLE IF NOT EXISTS "color_ranges" (
	"id" INTEGER NOT NULL,
	"name" TEXT NOT NULL,
	PRIMARY KEY("id"),
	FOREIGN KEY ("id") REFERENCES "color"("color_range_id")
	ON UPDATE NO ACTION ON DELETE NO ACTION
);

CREATE TABLE IF NOT EXISTS "embedded_metadata" (
	"id" INTEGER NOT NULL,
	"encodes_count" INTEGER NOT NULL,
	"last_encode_datetime_utc" DATETIME NOT NULL,
	"source_video_sha256_hash" TEXT NOT NULL,
	"environment_id" INTEGER NOT NULL,
	PRIMARY KEY("id"),
	FOREIGN KEY ("environment_id") REFERENCES "environment"("id")
	ON UPDATE NO ACTION ON DELETE NO ACTION
);

CREATE TABLE IF NOT EXISTS "job" (
	"id" INTEGER NOT NULL,
	"source_video_id" INTEGER NOT NULL,
	"stage_id" INTEGER NOT NULL,
	"created_datetime_utc" DATETIME NOT NULL,
	"total_time_seconds" REAL,
	PRIMARY KEY("id"),
	FOREIGN KEY ("stage_id") REFERENCES "job_stages"("id")
	ON UPDATE NO ACTION ON DELETE NO ACTION
);

CREATE TABLE IF NOT EXISTS "video" (
	"id" INTEGER NOT NULL,
	"file_id" INTEGER,
	"display_id" INTEGER,
	"playback_id" INTEGER,
	"encoding_id" INTEGER,
	"color_id" INTEGER,
	"embedded_metadata_id" INTEGER,
	PRIMARY KEY("id"),
	FOREIGN KEY ("file_id") REFERENCES "file"("id")
	ON UPDATE NO ACTION ON DELETE NO ACTION,
	FOREIGN KEY ("display_id") REFERENCES "display"("id")
	ON UPDATE NO ACTION ON DELETE NO ACTION,
	FOREIGN KEY ("playback_id") REFERENCES "playback"("id")
	ON UPDATE NO ACTION ON DELETE NO ACTION,
	FOREIGN KEY ("encoding_id") REFERENCES "encoding"("id")
	ON UPDATE NO ACTION ON DELETE NO ACTION,
	FOREIGN KEY ("color_id") REFERENCES "color"("id")
	ON UPDATE NO ACTION ON DELETE NO ACTION,
	FOREIGN KEY ("embedded_metadata_id") REFERENCES "embedded_metadata"("id")
	ON UPDATE NO ACTION ON DELETE NO ACTION,
	FOREIGN KEY ("id") REFERENCES "job"("source_video_id")
	ON UPDATE NO ACTION ON DELETE NO ACTION
);

CREATE TABLE IF NOT EXISTS "segments" (
	"id" INTEGER NOT NULL,
	"job_id" INTEGER NOT NULL,
	"from_frame" INTEGER NOT NULL,
	"to_frame" INTEGER NOT NULL,
	"status_id" INTEGER NOT NULL,
	"total_time_seconds" REAL,
	PRIMARY KEY("id"),
	FOREIGN KEY ("status_id") REFERENCES "segment_statuses"("id")
	ON UPDATE NO ACTION ON DELETE NO ACTION,
	FOREIGN KEY ("job_id") REFERENCES "job"("id")
	ON UPDATE NO ACTION ON DELETE NO ACTION
);

CREATE TABLE IF NOT EXISTS "iteration" (
	"id" INTEGER NOT NULL,
	"segment_id" INTEGER NOT NULL,
	"stage_id" INTEGER NOT NULL,
	"video_id" INTEGER NOT NULL,
	"cpu_id" INTEGER NOT NULL,
	"environment_id" INTEGER NOT NULL,
	"execution_data_id" INTEGER NOT NULL,
	"crf" INTEGER NOT NULL,
	PRIMARY KEY("id"),
	FOREIGN KEY ("segment_id") REFERENCES "segments"("id")
	ON UPDATE NO ACTION ON DELETE NO ACTION,
	FOREIGN KEY ("video_id") REFERENCES "video"("id")
	ON UPDATE NO ACTION ON DELETE NO ACTION,
	FOREIGN KEY ("cpu_id") REFERENCES "cpu"("id")
	ON UPDATE NO ACTION ON DELETE NO ACTION,
	FOREIGN KEY ("environment_id") REFERENCES "environment"("id")
	ON UPDATE NO ACTION ON DELETE NO ACTION,
	FOREIGN KEY ("stage_id") REFERENCES "iteration_stages"("id")
	ON UPDATE NO ACTION ON DELETE NO ACTION
);

CREATE TABLE IF NOT EXISTS "execution_data" (
	"id" INTEGER NOT NULL,
	"ffmpeg_command_used" TEXT NOT NULL,
	"finished_datetime_utc" DATETIME NOT NULL,
	"encoding_time_seconds" REAL NOT NULL,
	"evaluation_time_seconds" REAL,
	"total_time_seconds" REAL,
	"encoding_cpu_threads_used" INTEGER NOT NULL,
	"evaluation_cpu_threads_used" INTEGER,
	PRIMARY KEY("id"),
	FOREIGN KEY ("id") REFERENCES "iteration"("execution_data_id")
	ON UPDATE NO ACTION ON DELETE NO ACTION
);

CREATE TABLE IF NOT EXISTS "evaluation" (
	"id" INTEGER NOT NULL,
	"iteration_id" INTEGER NOT NULL,
	"metric_id" INTEGER NOT NULL,
	"score" REAL NOT NULL,
	PRIMARY KEY("id"),
	FOREIGN KEY ("iteration_id") REFERENCES "iteration"("id")
	ON UPDATE NO ACTION ON DELETE NO ACTION,
	FOREIGN KEY ("metric_id") REFERENCES "evaluation_metrics"("id")
	ON UPDATE NO ACTION ON DELETE NO ACTION
);
