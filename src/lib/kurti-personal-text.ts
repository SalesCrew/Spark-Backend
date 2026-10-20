/** Local, deliberately narrow safeguards for recognisable personal text outside the account directory.
 * No network/model is involved. This is not a general name detector or anonymisation guarantee.
 */
const NAME_WORD = String.raw`\p{Lu}[\p{Ll}\p{M}]+(?:[-’']\p{Lu}?[\p{Ll}\p{M}]+)*`;
const BUSINESS_WORDS = new Set([
  "coke", "spark", "kurti", "billa", "plus", "spar", "eurospar", "interspar", "rewe", "adeg", "hofer", "lidl", "penny", "metro", "unimarkt",
  "dashboard", "markt", "markte", "marktbesuche", "besuchte", "besuche", "durcharbeit", "fragebogen", "kampagne", "kampagnen", "basistatigkeiten", "leistungsstunden", "stunden", "kilometer", "merchandizer", "warenverfugbarkeit", "behebungsquote", "handelskette", "handelsketten", "anfragen", "offene", "zeitraum", "filter", "bonus", "pramien", "monatsstand",
  "montag", "dienstag", "mittwoch", "donnerstag", "freitag", "samstag", "sonntag", "janner", "januar", "februar", "marz", "april", "mai", "juni", "juli", "august", "september", "oktober", "november", "dezember",
  "zeige", "zeig", "berechne", "erstelle", "vergleiche", "sortiere", "erklare", "was", "wie", "wer", "wo", "welche", "welcher", "welches", "warum", "bitte", "alle", "aller", "fur", "von", "bis", "und", "mit", "vor", "nach", "ist", "sind", "der", "die", "das", "im", "pro", "ein", "eine", "hat", "haben", "kann", "kannst", "ich",
  "show", "compare", "count", "tell", "list", "how", "what", "who", "which", "give", "get", "create", "export", "calculate", "draw", "plot", "please", "by", "per", "for", "and", "with", "before", "after", "all", "the", "in", "of", "over", "time", "visits", "hours", "markets", "employees", "average", "availability", "results",
]);
const fold = (value: string) => value.normalize("NFKD").replace(/\p{M}/gu, "").toLowerCase();
const isBusinessWord = (value: string) => BUSINESS_WORDS.has(fold(value));
const personalIntroduction = new RegExp(String.raw`\b((?:[Mm]ein [Nn]ame ist|[Ii]ch hei(?:ß|ss)e|[Mm]y name is|[Hh]err|[Ff]rau|Mr\.?|Mrs\.?|Ms\.?))\s+(${NAME_WORD}(?:[ \t]+${NAME_WORD}){0,2})`, "gu");
// Catch bare title-case name lists; skip business terms and ordinary question/command words.
const bareNameList = new RegExp(String.raw`^\s*${NAME_WORD}(?:[\s,;]+${NAME_WORD}){3,}\s*[.!?]?\s*$`, "u");

export function redactRecognisablePersonalText(value: string): string {
  let text = value.replace(personalIntroduction, (match, introduction: string, names: string) => {
    if (names.split(/\s+/u).some(isBusinessWord)) return match;
    return `${introduction} [Person]`;
  });
  if (bareNameList.test(text)) {
    const words = text.match(new RegExp(NAME_WORD, "gu")) ?? [];
    if (words.every(word => !isBusinessWord(word))) text = "[Personen ohne Auswahl]";
  }
  return text
    .replace(/[^\s<>"']+@[^\s<>"']+\.[a-z]{2,}/gi, "[Kontakt entfernt]")
    .replace(/\b((?:[Tt]el(?:efon)?|[Mm]obil|[Pp]hone)\s*[:=]\s*)\+?[\d(][\d ()/.-]{6,}\d/g, "$1[Kontakt entfernt]");
}
