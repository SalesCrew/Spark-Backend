import { createHash } from "node:crypto";

// A separate immutable version: changing SM wording must never invalidate GM acceptances.
const document = {
  key: "spark_sm_employee_agreement",
  version: "2026-08-31-sm-v1",
  title: "Nutzungs- und Datenschutzvereinbarung für Shelf Merchandising",
  effectiveDate: "2026-08-31",
  sections: [
    {
      title: "1. Dein Einsatz mit Coke Spark",
      body: [
        "Coke Spark unterstützt dich als Shelf Merchandiser bei deinen zugewiesenen Markteinsätzen. Du bearbeitest Fragebögen, dokumentierst Ergebnisse und erfasst deine Besuchszeiten. Die App ist ein Arbeitsmittel und nicht für private Nutzung bestimmt.",
        "Verantwortlich ist die Institut für Verkaufsförderung GmbH, Wagenseilgasse 5, 1120 Wien. Bei Datenschutzfragen erreichst du die zuständige Stelle unter datenschutz@merch.at.",
      ],
    },
    {
      title: "2. Welche SM-Daten verarbeitet werden",
      body: [
        "Verarbeitet werden deine Kontaktdaten und Rolle, Marktzuordnungen, geplante Einsätze, Fragebogenantworten, erforderliche Fotos, Besuchszeiten, Nachrichten und deren Lesestatus sowie Korrektur- und Löschanfragen. Hinzu kommen die für Sicherheit und Nachvollziehbarkeit erforderlichen Login-, Vereinbarungs-, Audit- und Fehlerdaten.",
        "Für einen Marktbesuch verwendest du den Timer oder trägst Start und Ende manuell ein. Fahrtzeit kannst du zusätzlich eingeben, wenn diese Funktion für dein SM-Konto aktiviert ist. Im SM-Fragebogen werden keine Kilometerstände abgefragt.",
        "Fotografiere nur die für den Einsatz erforderlichen Markt- und Produktinhalte. Vermeide Personen, private Unterlagen und andere unnötige personenbezogene Informationen. Melde entsprechende Fehlaufnahmen zur Prüfung.",
      ],
    },
    {
      title: "3. Auswertung deiner abgeschlossenen Besuche",
      body: [
        "Die Daten dienen der Einsatzplanung, Arbeitsausführung, Zeitdokumentation, Nachweisführung, Qualitätssicherung, Berichterstattung, Fehlerkorrektur und Systemsicherheit.",
        "Aus vollständig abgeschlossenen Fragebögen werden gefundene Out-of-Stock-Fälle (OOS, also Regallücken), deren Behebung und die Anteile betroffener Märkte ausgewertet. Einzelne gespeicherte Antworten und noch offene Fragebögen zählen nicht in diese Kennzahlen.",
        "Deine SM-Startseite zeigt deine eigenen heutigen Einsätze und abgeschlossenen OOS-Ergebnisse. Berechtigte interne Stellen können die SM-Ergebnisse für ihre Aufgaben prüfen und auswerten. Es findet keine dauerhafte Live-Ortung statt.",
      ],
    },
    {
      title: "4. Wer Zugriff hat",
      body: [
        "In deinem SM-Zugang siehst du deine eigenen Einsätze, Antworten, Zeiten, Nachrichten und Anfragen. Die operativen SM- und GM-Daten sind getrennt.",
        "Berechtigte interne Admins und SM-Admins können SM-Daten im erforderlichen Umfang für Planung, Support, Prüfung, Reporting und Abrechnung einsehen. Die operative SM-API gibt Coca-Cola Kunden-Zugängen keinen direkten Zugriff. Externe Weitergaben und Exporte benötigen einen gesondert kontrollierten, zweckgebundenen Prozess.",
        "Technische Dienstleister verarbeiten Daten im erforderlichen Umfang für Anmeldung, Datenbank, privaten Dateispeicher, Hosting und Sicherheit. Fotos sind nicht öffentlich und werden über zeitlich begrenzte Links angezeigt.",
      ],
    },
    {
      title: "5. Wenn du offline arbeitest",
      body: [
        "Für schlechte Verbindungen speichert der Browser deine geladenen Planungs- und Fragebogendaten sowie noch nicht synchronisierte Antworten nutzergebunden auf deinem Gerät. Der lokale Zwischenspeicher verfällt spätestens nach 30 Tagen. Besuchsdaten werden bei Abschluss oder Verwerfen bereinigt; beim Logout oder Kontowechsel werden die zugehörigen lokalen SM-Zwischenspeicher entfernt.",
        "Noch nicht synchronisierte Antworten sind noch nicht vollständig auf dem Server gespeichert. Für den endgültigen Abschluss ist eine Verbindung nötig. Melde dich auf gemeinsam genutzten Geräten nach der Arbeit ab.",
      ],
    },
    {
      title: "6. Zeiten und Antworten korrigieren",
      body: [
        "Du kannst abgeschlossene Besuche unter Aktivitäten und deine Zeiten unter Zeiterfassung einsehen. Fehler in Antworten, Besuchen oder Zeiten kannst du über die vorgesehenen SM-Anfragen zur Prüfung einreichen.",
        "Bei einer Zeitkorrektur gibst du die gewünschten Start- und Endzeitpunkte und eine Begründung an. Eine beantragte Änderung oder Löschung wird erst nach Freigabe wirksam. Original, Antrag und Entscheidung bleiben für den erforderlichen Nachweiszeitraum nachvollziehbar.",
      ],
    },
    {
      title: "7. Aufbewahrung und deine Rechte",
      body: [
        "Es gelten die bestehenden internen SM-Aufbewahrungsregeln: Einsätze, Fragebögen, Antworten und Fotos grundsätzlich 3 Jahre nach Ende des betreffenden Kampagnen- oder Berichtsjahres; Besuchs-, Fahrt- und Korrekturzeiten bis zu 7 Jahre, soweit Arbeitszeit, Payroll, Aufwandsersatz oder buchhalterischer Nachweis betroffen ist. Nachrichten und Lesestatus werden nur solange erforderlich, grundsätzlich höchstens 3 Jahre, aufbewahrt.",
        "Login- und Sicherheitslogs werden grundsätzlich 24 Monate gespeichert. Technische Fehlerdetails werden grundsätzlich 90 Tage, aggregierte technische Statistiken höchstens 12 Monate aufbewahrt. Vereinbarungsnachweise bleiben für den aktiven Einsatz plus 3 Jahre erhalten. Export-Arbeitskopien sind grundsätzlich innerhalb von 30 Tagen nach Zweckerfüllung zu löschen.",
        "Nach Ende des aktiven Einsatzes und der operativen Übergabe wird der Login deaktiviert und das Profil grundsätzlich innerhalb von 30 Tagen anonymisiert. Gesetzliche Pflichten, offene Prüfungen, Ansprüche oder ein dokumentierter Legal Hold können eine längere Aufbewahrung erfordern.",
        "Du kannst Auskunft, Berichtigung, Löschung, Einschränkung, Datenübertragbarkeit und Widerspruch verlangen, soweit die gesetzlichen Voraussetzungen vorliegen. Wende dich dafür an datenschutz@merch.at. Ein Beschwerderecht besteht bei der österreichischen Datenschutzbehörde (dsb.gv.at). Die vollständige SM-Datenschutzinformation erläutert auch die Rechtsgrundlagen und Fristen.",
      ],
    },
    {
      title: "8. Bestätigung",
      body: [
        "Mit deiner Bestätigung erklärst du, dass du diese Vereinbarung gelesen hast und der Nutzung von Coke Spark als Arbeits-, Reporting- und Kontrollsystem für deine SM-Einsätze im beschriebenen Umfang zustimmst.",
        "Die Bestätigung ersetzt nicht die Rechtsgrundlagen der Datenverarbeitung. Die normale SM-Arbeitsausführung stützt sich auf die in der SM-Datenschutzinformation beschriebenen Grundlagen, nicht vorrangig auf eine datenschutzrechtliche Einwilligung.",
      ],
    },
  ],
};

export const SM_EMPLOYEE_AGREEMENT = {
  ...document,
  hash: createHash("sha256").update(JSON.stringify(document)).digest("hex"),
};
