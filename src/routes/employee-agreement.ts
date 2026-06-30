import { createHash } from "node:crypto";
import { and, desc, eq } from "drizzle-orm";
import { Router } from "express";
import { z } from "zod";
import { db } from "../lib/db.js";
import { employeeAgreementAcceptances } from "../lib/schema.js";
import { requireAuth, type AuthedRequest } from "../middleware/auth.js";

const AGREEMENT_KEY = "spark_employee_agreement";
const AGREEMENT_VERSION = "2026-06-29-v3";
const AGREEMENT_TITLE = "Nutzungs- und Kontrollmaßnahmenvereinbarung für Coke Spark";

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
    ],
  },
  {
    title: "3. Zweck der Kontrolle und Auswertung",
    body: [
      "Die Daten werden genutzt für Arbeitsausführung, Einsatzsteuerung, gesetzliche Arbeitszeitaufzeichnungen, Nachweisführung, Qualitätssicherung, Kundenreporting, Bonus-/Prämienberechnung, Fehlerkorrekturen, Support, Betrugs- und Missbrauchsvermeidung sowie Systemsicherheit.",
      "Spark wird nicht für verdeckte Überwachung eingesetzt. Es findet keine dauerhafte Live-Ortung statt, sofern eine solche Funktion nicht ausdrücklich gesondert vereinbart und dokumentiert wird.",
    ],
  },
  {
    title: "4. Wer Daten sehen kann",
    body: [
      "Berechtigte interne Admins, verantwortliche Manager und zuständige Abrechnungs-/HR-nahe Stellen können Daten sehen, soweit sie diese für ihre Aufgabe benötigen.",
      "Coca-Cola erhält nur die für das vereinbarte Reporting erforderlichen Markt-, Kampagnen-, Antwort-, Foto- und Statusdaten. Eine namentliche GM-Zuordnung kann sichtbar sein, soweit sie für Rückfragen, Kampagnenkontrolle oder Nachweisführung erforderlich ist.",
      "Coca-Cola erhält keinen Zugriff auf interne Arbeitszeit-, Kilometer-, Bonus-/Prämien-, HR-, Payroll- oder interne Sicherheits-/Auditdetails, sofern dies nicht ausdrücklich separat dokumentiert und freigegeben wird.",
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
      "Daten werden nur so lange gespeichert, wie sie für Arbeitszeitnachweise, Abrechnung, Kundenreporting, Qualitätssicherung, Kampagnenhistorie, Reklamationen, Sicherheit oder rechtliche Nachweise erforderlich sind.",
      "Arbeitszeit- und Korrekturdaten werden mindestens so lange aufbewahrt, wie gesetzliche Nachweis-, Abrechnungs- und arbeitsrechtliche Aufbewahrungspflichten bestehen oder Ansprüche daraus geprüft werden können.",
      "Coke Spark nutzt rollenbasierte Berechtigungen, serverseitige Zugriffskontrollen, private Speicherbereiche, signierte Datei-URLs, Protokollierung und Backend-only Datenbankzugriffe.",
    ],
  },
  {
    title: "7. Löschung und Anonymisierung",
    body: [
      "Eingereichte Marktbesuche, Antworten, Fotos, Zeit- und Kilometerdaten werden im System grundsätzlich als historische Arbeits-, Reporting- und Statistikdaten erhalten, weil sie für Nachweise, Auswertungen, Kundenreporting und Abrechnung erforderlich sein können.",
      "Wenn ein Mitarbeiter aus dem aktiven Einsatz ausscheidet und die operative Spark-Identität nicht mehr benötigt wird, kann der personenbezogene Nutzerstammdatensatz anonymisiert werden. Dabei werden Name, E-Mail-Adresse, Telefon, Adresse, PLZ/Ort, Region, Profilfoto und Loginbezug entfernt oder durch neutrale Platzhalter ersetzt.",
      "Die historischen Einträge bleiben dadurch für Statistiken und Nachweise erhalten, sind aber in Spark nicht mehr mit dem ursprünglichen Namen oder privaten Kontaktdaten sichtbar. Separate HR-/Payroll-Unterlagen außerhalb von Spark können aufgrund gesetzlicher Pflichten länger personenbezogen aufbewahrt werden.",
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
  .update(JSON.stringify({ key: AGREEMENT_KEY, version: AGREEMENT_VERSION, title: AGREEMENT_TITLE, sections: agreementSections }))
  .digest("hex");

const acceptSchema = z.object({
  version: z.string().min(1),
});

function agreementPayload() {
  return {
    key: AGREEMENT_KEY,
    version: AGREEMENT_VERSION,
    title: AGREEMENT_TITLE,
    hash: agreementHash,
    effectiveDate: "2026-06-29",
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
          eq(employeeAgreementAcceptances.agreementKey, AGREEMENT_KEY),
          eq(employeeAgreementAcceptances.agreementVersion, AGREEMENT_VERSION),
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

    if (parsed.data.version !== AGREEMENT_VERSION) {
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
          eq(employeeAgreementAcceptances.agreementKey, AGREEMENT_KEY),
          eq(employeeAgreementAcceptances.agreementVersion, AGREEMENT_VERSION),
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
              agreementKey: AGREEMENT_KEY,
              agreementVersion: AGREEMENT_VERSION,
              agreementTitle: AGREEMENT_TITLE,
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
