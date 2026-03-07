export const TEAM_SEARCH_ALIASES: Record<string, string[]> = {
  ARI: ["ARI", "Arizona", "Arizona Cardinals", "Cardinals"],
  ATL: ["ATL", "Atlanta", "Atlanta Falcons", "Falcons"],
  BAL: ["BAL", "Baltimore", "Baltimore Ravens", "Ravens"],
  BUF: ["BUF", "Buffalo", "Buffalo Bills", "Bills"],
  CAR: ["CAR", "Carolina", "Carolina Panthers", "Panthers"],
  CHI: ["CHI", "Chicago", "Chicago Bears", "Bears"],
  CIN: ["CIN", "Cincinnati", "Cincinnati Bengals", "Bengals"],
  CLE: ["CLE", "Cleveland", "Cleveland Browns", "Browns"],
  DAL: ["DAL", "Dallas", "Dallas Cowboys", "Cowboys"],
  DEN: ["DEN", "Denver", "Denver Broncos", "Broncos"],
  DET: ["DET", "Detroit", "Detroit Lions", "Lions"],
  GB: ["GB", "Green Bay", "Green Bay Packers", "Packers"],
  HOU: ["HOU", "Houston", "Houston Texans", "Texans"],
  IND: ["IND", "Indianapolis", "Indianapolis Colts", "Colts"],
  JAX: ["JAX", "Jacksonville", "Jacksonville Jaguars", "Jaguars"],
  KC: ["KC", "Kansas City", "Kansas City Chiefs", "Chiefs"],
  LAC: ["LAC", "Los Angeles Chargers", "LA Chargers", "Chargers"],
  LAR: ["LAR", "Los Angeles Rams", "LA Rams", "Rams"],
  LV: ["LV", "Las Vegas", "Las Vegas Raiders", "Raiders"],
  MIA: ["MIA", "Miami", "Miami Dolphins", "Dolphins"],
  MIN: ["MIN", "Minnesota", "Minnesota Vikings", "Vikings"],
  NE: ["NE", "New England", "New England Patriots", "Patriots"],
  NO: ["NO", "New Orleans", "New Orleans Saints", "Saints"],
  NYG: ["NYG", "New York Giants", "Giants"],
  NYJ: ["NYJ", "New York Jets", "Jets"],
  PHI: ["PHI", "Philadelphia", "Philadelphia Eagles", "Eagles"],
  PIT: ["PIT", "Pittsburgh", "Pittsburgh Steelers", "Steelers"],
  SEA: ["SEA", "Seattle", "Seattle Seahawks", "Seahawks"],
  SF: ["SF", "San Francisco", "San Francisco 49ers", "49ers", "Niners"],
  TB: ["TB", "Tampa Bay", "Tampa Bay Buccaneers", "Buccaneers", "Bucs"],
  TEN: ["TEN", "Tennessee", "Tennessee Titans", "Titans"],
  WAS: ["WAS", "Washington", "Washington Commanders", "Commanders"],
};

export function normalizeSearch(value: string | number | null | undefined) {
  return String(value || "")
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function teamDisplayName(team: string) {
  const aliases = TEAM_SEARCH_ALIASES[String(team || "").trim()] || [team];
  return (
    aliases
      .filter((item) => item.includes(" ") && item.toUpperCase() !== String(team || "").toUpperCase())
      .sort((a, b) => b.length - a.length)[0] || String(team || "").trim()
  );
}

export function teamSlug(team: string) {
  return normalizeSearch(teamDisplayName(team)).replace(/ /g, "-");
}

export function teamPath(team: string) {
  return `/nfl-team-needs-2026/${teamSlug(team)}`;
}

export function designationClass(label: string) {
  return `designation designation--${String(label || "backup")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "")}`;
}

export function txClass(kind: string) {
  return String(kind || "").toLowerCase() === "confirmed" ? "chip good" : "chip warn";
}

export function laneFor(lanes: any[], pos: string) {
  return (lanes || []).find((lane: any) => String(lane.position || "").toUpperCase() === pos) || { position: pos, players: [] };
}

export function lanePlayers(lane: any, limit = 2) {
  return ((lane?.players || []) as any[]).slice(0, limit);
}

export function roleContractLine(player: any) {
  const role = String(player?.role_label || player?.position || "").trim();
  const contract = String(player?.contract_label || "").trim() || "FA";
  return [role, contract].filter(Boolean).join(" • ");
}

export function metaLine(player: any) {
  return String(player?.meta_label || "").trim();
}

export function contractChipLabel(player: any) {
  const contract = String(player?.contract_label || "").trim();
  if (!contract || contract.toUpperCase() === "FA") return "FA";
  if (contract.toLowerCase().includes("watch")) return "Watch";
  const match = contract.match(/^(\d+)y/i);
  if (!match) return "Rostered";
  const years = Number(match[1] || 0);
  if (years <= 1) return "1y";
  if (years === 2) return "2y";
  return `${years}y`;
}

export function contractChipClass(player: any) {
  const label = contractChipLabel(player);
  if (label === "FA" || label === "1y" || label === "Watch") return "chip warn contract-chip";
  if (label === "2y") return "chip info contract-chip";
  return "chip good contract-chip";
}

export function searchableText(row: any) {
  const team = String(row.team || "").trim();
  const aliases = TEAM_SEARCH_ALIASES[team] || [team];
  const weakness = (row.weakness_positions || []) as string[];
  const freeAgents = (row.free_agents || []) as any[];
  const freeAgentsFull = (row.free_agents_full || []) as any[];
  const youngPlayers = (row.young_players_on_rise || []) as any[];
  const depthChart = (row.depth_chart || {}) as any;
  const offense = (depthChart.offense || []) as any[];
  const defense = (depthChart.defense || []) as any[];
  return [
    team,
    ...aliases,
    ...weakness,
    ...freeAgents.map((p) => p.player_name),
    ...freeAgentsFull.map((p) => p.player_name),
    ...youngPlayers.map((p) => p.player_name),
    ...offense.flatMap((lane) => (lane.players || []).map((p: any) => p.player_name)),
    ...defense.flatMap((lane) => (lane.players || []).map((p: any) => p.player_name)),
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();
}
