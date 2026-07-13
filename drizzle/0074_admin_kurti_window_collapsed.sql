ALTER TABLE public.admin_kurti_window_preferences
  ADD COLUMN IF NOT EXISTS is_collapsed boolean DEFAULT false NOT NULL;
