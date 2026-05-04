# Automation setup (GitHub Pages, no Power BI)

## Repo layout
```
repo-root/
  factu.py
  index.html
  requirements.txt
  data/                    # auto-generated, committed by the workflow
  .github/workflows/refresh.yml
```

## 1. Push the files
Commit `factu.py`, `index.html`, `requirements.txt`, and `.github/workflows/refresh.yml` to the repo. Leave `data/` empty; the workflow will populate it.

## 2. GitHub secrets
Repo -> Settings -> Secrets and variables -> Actions -> New repository secret. Add:
- EVO_CO_USER, EVO_CO_PASS
- EVO_MX_USER, EVO_MX_PASS
- EVO_BR_USER, EVO_BR_PASS

## 3. Enable Pages
Repo -> Settings -> Pages -> Build and deployment -> Source: `GitHub Actions`.

## 4. Allow Actions to write
Repo -> Settings -> Actions -> General -> Workflow permissions -> "Read and write permissions" -> Save.

## 5. First run
Actions tab -> `factu-refresh` -> Run workflow. It will:
1. Run `factu.py` and write CSVs into `data/`.
2. Commit the CSVs.
3. Deploy the dashboard to Pages.

URL: `https://<your-user>.github.io/<repo>/`

## Schedule
Daily at 08:00 UTC (03:00 Bogota). Edit cron in `.github/workflows/refresh.yml` to change.

## Cost
GitHub Free covers it. Public repo = unlimited Actions and Pages. Private repo = 2000 Actions minutes/month free, more than enough for one daily job.

## Vercel alternative
If you prefer Vercel: import the repo, set Output Directory to repo root. Vercel rebuilds automatically when the workflow commits new CSVs. No Vercel Blob needed because CSVs are static files in the repo.
