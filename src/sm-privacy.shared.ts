export type SmDsarCounts = {
  assignedMarkets: number;
  assignments: number;
  submissions: number;
  answers: number;
  photos: number;
  timeRecords: number;
  messages: number;
  answerChangeRequests: number;
  submissionDeleteRequests: number;
  timeChangeRequests: number;
  auditEvents: number;
  securityRecords: number;
};

export type DsarCategory = {
  key: string;
  label: string;
  count: number;
  retention: string;
  actionHint: string;
};

export function buildSmDsarCategories(counts: SmDsarCounts): DsarCategory[] {
  return [
    {
      key: "profile",
      label: "SM-Profil und Account",
      count: 1,
      retention: "Aktiver Einsatz; nach Offboarding wird der Zugang deaktiviert und der SM-Stammdatensatz grundsätzlich innerhalb von 30 Tagen anonymisiert.",
      actionHint: "Korrektur über SM-Benutzerverwaltung; Anonymisierung erst nach dokumentierter operativer Übergabe.",
    },
    {
      key: "sm_planning",
      label: "SM-Marktzuordnungen und Einsätze",
      count: counts.assignedMarkets + counts.assignments,
      retention: "Operative Planung nur solange erforderlich; abgeschlossene Einsatznachweise entsprechend der zugehörigen Zeit-/Reportingfrist.",
      actionHint: "Aktive Zuordnungen prüfen; historische Nachweise nicht ungeprüft entfernen.",
    },
    {
      key: "sm_visits",
      label: "SM-Marktbesuche und Fragebögen",
      count: counts.submissions,
      retention: "Grundsätzlich 3 Jahre nach Ende des betreffenden Kampagnen- oder Berichtsjahres, sofern kein Legal Hold oder Nachweisinteresse entgegensteht.",
      actionHint: "Berichtigung/Löschung über die SM-Aktivitäten- und Anfrageprozesse prüfen.",
    },
    {
      key: "sm_answers",
      label: "SM-Fragebogenantworten",
      count: counts.answers,
      retention: "Gemeinsam mit dem zugehörigen Marktbesuch grundsätzlich 3 Jahre.",
      actionHint: "Antwortkorrekturen bleiben versioniert und nachvollziehbar; Rechte Dritter vor Herausgabe prüfen.",
    },
    {
      key: "sm_photos",
      label: "SM-Besuchsfotos",
      count: counts.photos,
      retention: "Grundsätzlich 3 Jahre; private oder sensible Fehlaufnahmen werden nach Prüfung früher entfernt oder eingeschränkt.",
      actionHint: "Storage-Datei und Metadaten gemeinsam prüfen; signierte URLs sind keine dauerhaften Exportlinks.",
    },
    {
      key: "sm_time",
      label: "SM-Besuchs- und Fahrtzeiten",
      count: counts.timeRecords,
      retention: "7 Jahre, soweit Arbeitszeit, Abrechnung, Aufwandsersatz oder buchhalterischer Nachweis betroffen ist.",
      actionHint: "Berichtigung über den SM-Zeitanfrageprozess; Original und genehmigte Korrektur bleiben nachvollziehbar.",
    },
    {
      key: "sm_messages",
      label: "SM-Nachrichten und Lesestatus",
      count: counts.messages,
      retention: "Nur solange für Einsatzinformation und Nachweis erforderlich; Sichtbarkeit nach dem Lesen wird fachlich begrenzt.",
      actionHint: "Nachrichteninhalt auf unnötige personenbezogene Angaben prüfen; Empfänger-Snapshots bei Anonymisierung bereinigen.",
    },
    {
      key: "sm_requests",
      label: "SM-Korrektur- und Löschanfragen",
      count: counts.answerChangeRequests + counts.submissionDeleteRequests + counts.timeChangeRequests,
      retention: "Antwort-/Löschanfragen grundsätzlich mit dem Besuch 3 Jahre; zeitrelevante Anfragen bis zu 7 Jahre.",
      actionHint: "Status, Entscheidung, Begründung und angewendete Änderung gemeinsam prüfen.",
    },
    {
      key: "sm_security",
      label: "SM-Audit-, Login- und Vereinbarungsnachweise",
      count: counts.auditEvents + counts.securityRecords,
      retention: "Login-/Sicherheitslogs grundsätzlich 24 Monate; Vereinbarungsnachweise aktiver Einsatz plus 3 Jahre.",
      actionHint: "Nur für Sicherheit, Rechenschaft und Nachvollziehbarkeit verwenden, nicht als verdeckte Leistungsbewertung.",
    },
  ];
}
