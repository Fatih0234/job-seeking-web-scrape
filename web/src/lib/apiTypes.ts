export type Platform = "linkedin" | "stepstone" | "xing";

export type CityBubble = {
  token_kind: "city";
  lat: number;
  lon: number;
  map_city_label: string;
  jobs_unique: number;
  jobs_weighted: number;
  new_24h_unique: number;
  new_7d_unique: number;
  new_30d_unique?: number;
  top_companies: string[] | null;
  top_skills: string[] | null;
};

export type MapPointJob = {
  platform: Platform;
  job_id: string;
  job_url: string | null;
  job_title: string | null;
  company_name: string | null;
  job_location: string | null;
  posted_at_utc: string | null;
  first_seen_at: string | null;
  extracted_skills: unknown | null;
};

export type MapPoint = MapPointJob & {
  token_kind: "city" | "remote";
  map_point_id: string;
  map_city_label: string;
  lat: number | null;
  lon: number | null;
  geocode_status: string | null;
  remote_scatter_ok: boolean | null;
};

export type JobDetail = {
  platform: Platform;
  job_id: string;
  job_url: string | null;
  job_title: string | null;
  company_name: string | null;
  job_location: string | null;
  job_description: string | null;
  posted_at_utc: string | null;
  first_seen_at: string | null;
  last_seen_at: string | null;
  extracted_skills: unknown | null;
};
