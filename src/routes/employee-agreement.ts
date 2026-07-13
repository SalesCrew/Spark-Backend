import { createHash } from "node:crypto";
import { and, desc, eq } from "drizzle-orm";
import { Router } from "express";
import { z } from "zod";
import { db } from "../lib/db.js";
import {
  EMPLOYEE_AGREEMENT_EFFECTIVE_DATE,
  EMPLOYEE_AGREEMENT_KEY,
  EMPLOYEE_AGREEMENT_TITLE,
  EMPLOYEE_AGREEMENT_VERSION,
} from "../lib/employee-agreement-meta.js";
import { employeeAgreementAcceptances } from "../lib/schema.js";
import { requireAuth, type AuthedRequest } from "../middleware/auth.js";

const agreementSections = [
  {
    title: "1. Zweck von Coke Spark",
    body: [
      "Coke Spark ist ein Arbeitsausführungs- und Reporting-System für die externe Field Force. Die App wird verwendet, um Marktbesuche, Kampagnen, Fragebögen, Fotos, Zeit- und Kilometerdaten sowie notwendige Nachweise zu planen, auszuführen, zu prüfen und gegenüber dem Auftraggeber Coca-Cola zu berichten.",
      "Die App ist ein Arbeitsmittel. Sie ist nicht für private Nutzung bestimmt.",
    ],
  },
  {
    title: "2. Welche Daten verarbeitet werden",
    body: [
      "Verarbeitet werden insbesondere Name, Rolle, E-Mail-Adresse, Region, Markt- und Kampagnenzuordnungen, Besuchszeiten, Start- und Endzeiten des Arbeitstags, Pausen, Zusatzzeiten, Kilometerstände, Fragebogenantworten, Fotos, Tags, Kommentare, Korrekturanfragen, Statusdaten, IPP-/Qualitätskennzahlen, Bonus-/Prämienwerte sowie technische Sicherheits- und Fehlerprotokolle.",
      "Arbeitszeitaufzeichnungen umfassen insbesondere Beginn und Ende des Arbeitstags, Pausen, Tages- und Wochenarbeitszeit, Zusatzzeiten, Marktbesuche, Anfahrt/Fahrtzeit/Heimfahrt, Kilometerstände und nachträglich freigegebene Korrekturen.",
      "Fotos sollen nur markt- und kampagnenrelevante Inhalte zeigen. Personen, private Unterlagen oder unnötige personenbezogene Details sollen nicht fotografiert werden.",
      "Wenn du Frag Kurti nutzt, werden deine Chatnachricht und ein begrenzter, für die Assistenz vorgesehener Ausschnitt aus deinen eigenen Spark-Daten an den eingesetzten KI-Dienstleister übermittelt. Dazu können insbesondere eigene Profilangaben, Arbeitszeit-/KM-Zusammenfassungen, Besuche, Kampagnen, offene Fragebögen und eigene Anfragestatus gehören. Daten anderer Mitarbeitender, Admin-Inhalte, Passwörter, Authentifizierungsdaten und technische Rohdaten sind nicht Bestandteil dieses Assistenten-Kontexts.",
    ],
  },
  {
    title: "3. Zweck der Kontrolle und Auswertung",
    body: [
      "Die Daten werden genutzt für Arbeitsausführung, Einsatzsteuerung, gesetzliche Arbeitszeitaufzeichnungen, Nachweisführung, Qualitätssicherung, Kundenreporting, Bonus-/Prämienberechnung, Fehlerkorrekturen, Support, Betrugs- und Missbrauchsvermeidung sowie Systemsicherheit.",
      "Spark wird nicht für verdeckte Überwachung eingesetzt. Es findet keine dauerhafte Live-Ortung statt, sofern eine solche Funktion nicht ausdrücklich gesondert vereinbart und dokumentiert wird.",
      "Frag Kurti dient ausschließlich als Lese-, Navigations- und Supporthilfe. Kurti kann keine Einträge ändern, nichts absenden und keine Personal-, Bonus- oder sonstige Entscheidung treffen. Maßgeblich bleiben immer die in Coke Spark angezeigten Daten und die Prüfung durch berechtigte interne Stellen.",
    ],
  },
  {
    title: "4. Wer Daten sehen kann",
    body: [
      "Berechtigte interne Admins, verantwortliche Manager und zuständige Abrechnungs-/HR-nahe Stellen können Daten sehen, soweit sie diese für ihre Aufgabe benötigen.",
      "Coca-Cola erhält nur die für das vereinbarte Reporting erforderlichen Markt-, Kampagnen-, Antwort-, Foto- und Statusdaten. Eine namentliche GM-Zuordnung kann sichtbar sein, soweit sie für Rückfragen, Kampagnenkontrolle oder Nachweisführung erforderlich ist.",
      "Coca-Cola erhält keinen Zugriff auf interne Arbeitszeit-, Kilometer-, Bonus-/Prämien-, HR-, Payroll- oder interne Sicherheits-/Auditdetails, sofern dies nicht ausdrücklich separat dokumentiert und freigegeben wird.",
      "Für Frag Kurti wird OpenAI als technischer KI-Dienstleister eingesetzt. Übermittelt werden nur die Nachricht des aktuell angemeldeten Nutzers, dessen eigener begrenzter Spark-Kontext und die Systemanweisungen für Navigation, Support, Datenschutz und Datengrenzen. Andere GM-Daten werden nicht bereitgestellt.",
    ],
  },
  {
    title: "5. Korrekturen und Rückfragen",
    body: [
      "Du kannst deine eigenen Arbeitszeitdaten in der App einsehen. Wenn ein Zeiteintrag falsch ist, kannst du über die vorgesehene Zeiterfassungs-Anfrage eine Korrektur vorschlagen.",
      "Die ursprüngliche Zeit und die beantragte neue Zeit werden für die Prüfung gespeichert. Erst nach Freigabe durch eine berechtigte interne Stelle wird die neue Zeit als maßgeblicher Wert für Anzeige, Auswertung und Export verwendet.",
      "Korrekturen, Ablehnungen und Freigaben werden protokolliert, damit Arbeitszeitaufzeichnungen nachvollziehbar bleiben.",
    ],
  },
  {
    title: "6. Speicherfristen und Sicherheit",
    body: [
      "Coke Spark speichert Daten nach einem internen Aufbewahrungskonzept. Marktbesuche, Fragebogenantworten, Kommentare, Tags, Status und Besuchsfotos werden grundsätzlich 3 Jahre nach Ende des betreffenden Kampagnen- oder RED-Jahres aufbewahrt und danach gelöscht oder anonymisiert, sofern keine Reklamation, Nachweispflicht oder Legal Hold besteht.",
      "Arbeitszeit-, Pausen-, Zusatzzeit-, Tagesstart/-ende- und Kilometerdaten werden 7 Jahre nach Ende des Kalenderjahres aufbewahrt, wenn sie für Abrechnung, Diäten, Payroll, Aufwandsersatz oder buchhalterische Nachweise verwendet werden. Zeitkorrekturanfragen werden für denselben Zeitraum aufbewahrt.",
      "Login-, Auth-, Audit- und Sicherheitslogs werden grundsätzlich 24 Monate gespeichert. Technische Telemetrie und Fehlerdetails werden grundsätzlich 90 Tage gespeichert; aggregierte technische Statistiken höchstens 12 Monate. Exportdateien sind Arbeitskopien und grundsätzlich innerhalb von 30 Tagen nach Zweckerfüllung zu löschen.",
      "Coke Spark nutzt rollenbasierte Berechtigungen, serverseitige Zugriffskontrollen, private Speicherbereiche, signierte Datei-URLs, Protokollierung und Backend-only Datenbankzugriffe.",
      "Kurti-Chatnachrichten werden in Coke Spark 15 Minuten nach der letzten erfolgreichen Unterhaltung gespeichert und danach gelöscht. Die Responses-API wird mit deaktivierter Anwendungsspeicherung verwendet. Davon getrennt können beim KI-Dienstleister Inhalte nach dessen vertraglichen Datenkontrollen standardmäßig bis zu 30 Tage in Missbrauchs-/Sicherheitsprotokollen und verschlüsselte Prompt-Cache-Zwischendaten bis zu 24 Stunden verarbeitet werden, sofern für das verwendete API-Projekt keine strengeren Aufbewahrungskontrollen aktiviert sind. API-Inhalte werden nicht zum Modelltraining verwendet, außer der Verantwortliche würde einer solchen Nutzung ausdrücklich zustimmen.",
    ],
  },
  {
    title: "7. Löschung und Anonymisierung",
    body: [
      "Wenn ein Mitarbeiter aus dem aktiven Einsatz ausscheidet und die operative Spark-Identität nicht mehr benötigt wird, wird der Spark-Login deaktiviert und der personenbezogene Nutzerstammdatensatz grundsätzlich innerhalb von 30 Tagen nach Abschluss der Übergabe anonymisiert, sofern keine offene Prüfung, Abrechnung, Reklamation oder gesetzliche Pflicht entgegensteht.",
      "Dabei werden Name, E-Mail-Adresse, Telefon, Adresse, PLZ/Ort, Region, Profilfoto und Loginbezug entfernt oder durch neutrale Platzhalter ersetzt. Historische Einträge bleiben nur im zulässigen Umfang für Statistiken, Nachweise, Kundenreporting und Abrechnung erhalten.",
      "Offensichtlich private oder sensible Fehlfotos sollen gemeldet werden und werden nach Prüfung früher gelöscht oder eingeschränkt. Gesetzliche Aufbewahrungspflichten, offene Ansprüche, Sicherheitsvorfälle oder ein dokumentierter Legal Hold können einzelne Löschfristen verlängern.",
    ],
  },
  {
    title: "8. Zustimmung",
    body: [
      "Mit dem Akzeptieren bestätige ich, dass ich diese Vereinbarung gelesen habe und der Nutzung von Coke Spark als technischem Arbeits-, Reporting- und Kontrollsystem im beschriebenen Umfang zustimme.",
      "Mir ist bekannt, dass die Nutzung von Coke Spark für die zugewiesene Field-Force-Arbeit erforderlich sein kann und dass die oben beschriebenen Daten zur Arbeitsausführung, Nachweisführung, Qualitätssicherung und zum Kundenreporting verarbeitet werden.",
    ],
  },
] as const;

const agreementHash = createHash("sha256")
  .update(JSON.stringify({ key: EMPLOYEE_AGREEMENT_KEY, version: EMPLOYEE_AGREEMENT_VERSION, title: EMPLOYEE_AGREEMENT_TITLE, sections: agreementSections }))
  .digest("hex");

const acceptSchema = z.object({
  version: z.string().min(1),
});

function agreementPayload() {
  return {
    key: EMPLOYEE_AGREEMENT_KEY,
    version: EMPLOYEE_AGREEMENT_VERSION,
    title: EMPLOYEE_AGREEMENT_TITLE,
    hash: agreementHash,
    effectiveDate: EMPLOYEE_AGREEMENT_EFFECTIVE_DATE,
    sections: agreementSections,
  };
}

export const employeeAgreementRouter = Router();

employeeAgreementRouter.get("/current", requireAuth(["gm", "sm"]), async (req: AuthedRequest, res, next) => {
  try {
    if (!req.authUser) {
      res.status(401).json({ error: "Authentication required.", code: "auth_required" });
      return;
    }

    const [acceptance] = await db
      .select()
      .from(employeeAgreementAcceptances)
      .where(
        and(
          eq(employeeAgreementAcceptances.userId, req.authUser.appUserId),
          eq(employeeAgreementAcceptances.agreementKey, EMPLOYEE_AGREEMENT_KEY),
          eq(employeeAgreementAcceptances.agreementVersion, EMPLOYEE_AGREEMENT_VERSION),
        ),
      )
      .orderBy(desc(employeeAgreementAcceptances.acceptedAt))
      .limit(1);

    res.status(200).json({
      agreement: agreementPayload(),
      accepted: Boolean(acceptance),
      acceptance: acceptance
        ? {
            acceptedAt: acceptance.acceptedAt.toISOString(),
            version: acceptance.agreementVersion,
            hash: acceptance.agreementHash,
          }
        : null,
    });
  } catch (err) {
    next(err);
  }
});

employeeAgreementRouter.post("/accept", requireAuth(["gm", "sm"]), async (req: AuthedRequest, res, next) => {
  try {
    if (!req.authUser) {
      res.status(401).json({ error: "Authentication required.", code: "auth_required" });
      return;
    }

    const parsed = acceptSchema.safeParse(req.body);
    if (!parsed.success) {
      res.status(400).json({ error: "Ungültige Vereinbarung.", code: "invalid_agreement_payload" });
      return;
    }

    if (parsed.data.version !== EMPLOYEE_AGREEMENT_VERSION) {
      res.status(409).json({
        error: "Diese Vereinbarung ist nicht mehr aktuell. Bitte aktuelle Version laden.",
        code: "agreement_version_mismatch",
        agreement: agreementPayload(),
      });
      return;
    }

    const [existing] = await db
      .select()
      .from(employeeAgreementAcceptances)
      .where(
        and(
          eq(employeeAgreementAcceptances.userId, req.authUser.appUserId),
          eq(employeeAgreementAcceptances.agreementKey, EMPLOYEE_AGREEMENT_KEY),
          eq(employeeAgreementAcceptances.agreementVersion, EMPLOYEE_AGREEMENT_VERSION),
        ),
      )
      .limit(1);

    const acceptance = existing
      ? existing
      : (
          await db
            .insert(employeeAgreementAcceptances)
            .values({
              userId: req.authUser.appUserId,
              agreementKey: EMPLOYEE_AGREEMENT_KEY,
              agreementVersion: EMPLOYEE_AGREEMENT_VERSION,
              agreementTitle: EMPLOYEE_AGREEMENT_TITLE,
              agreementHash,
              acceptedIp: req.ip ?? req.socket.remoteAddress ?? null,
              acceptedUserAgent: req.get("user-agent")?.slice(0, 512) ?? null,
            })
            .returning()
        )[0];

    if (!acceptance) {
      throw new Error("Employee agreement acceptance could not be persisted.");
    }

    res.status(200).json({
      accepted: true,
      acceptance: {
        acceptedAt: acceptance.acceptedAt.toISOString(),
        version: acceptance.agreementVersion,
        hash: acceptance.agreementHash,
      },
    });
  } catch (err) {
    next(err);
  }
});
