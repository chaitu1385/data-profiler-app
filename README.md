# Data Profiler

An interactive, client-side CSV data profiler that runs entirely in the browser. Upload a CSV file and instantly get summary statistics, auto-selected charts, and actionable insights — no server required.

## Features

- **Drag-and-drop CSV upload** — click or drop a `.csv` file to begin profiling
- **Automatic type detection** — identifies numeric, categorical, boolean, date, and text columns
- **Summary statistics** — count, unique values, missing %, min/max, mean, median, standard deviation, quartiles, and more
- **Sortable data table** — browse the raw data with column sorting
- **Auto-selected charts** — generates histograms, bar charts, pie charts, and scatter plots based on detected column types
- **Top insights** — highlights missing data, outliers, correlations, skewness, dominant categories, and high-cardinality columns

## Getting Started

No build step or dependencies to install. Simply open `index.html` in a modern web browser:

```bash
open index.html
```

Or serve it locally with any static file server:

```bash
npx serve .
```

Then drag and drop a CSV file onto the upload zone.

## Technologies

- [PapaParse](https://www.papaparse.com/) — CSV parsing
- [Chart.js](https://www.chartjs.org/) — chart rendering

Both libraries are loaded from CDN, so an internet connection is required on first load.

## License

This project is provided as-is for personal and educational use.
