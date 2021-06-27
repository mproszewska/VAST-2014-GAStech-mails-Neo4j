#!/usr/bin/env python
import os
from json import dumps
import logging

from flask import Flask, g, Response, request
from neo4j import GraphDatabase, basic_auth

app = Flask(__name__, static_url_path="/static/")

url = os.getenv("NEO4J_URI", "neo4j://localhost:7687")
username = os.getenv("NEO4J_USER", "neo4j")
password = os.getenv("NEO4J_PASSWORD", "pass")
neo4jVersion = os.getenv("NEO4J_VERSION", "")
port = os.getenv("PORT", 8080)

driver = GraphDatabase.driver(url, auth=basic_auth(username, password))


def neo4j_date_to_string(date):
    date = str(date).replace("T", "-").replace(":", "-").split("-")
    out = f"{date[0]}-{date[1]}-{date[2]} {date[3]}:{date[4]}"
    return out


def neo4j_str_date_to_string(date):
    date = str(date).split("/")
    out = f"{date[2]}-{date[0]}-{date[1]}"
    return out


def neo4j_date_from_string(date):
    if ":" not in date:
        date = date + " 00:00"
    date = str(date).replace(":", "-").replace(" ", "-").split("-")
    out = f"{date[0]}-{date[1]}-{date[2]}T{date[3]}:{date[4]}:00Z"
    return out


def parse_dates(date_start, date_end):
    if date_start == "":
        date_start = "2000-01-01 00:00"
    if date_end == "":
        date_end = "2020-01-01 00:00"
    date_start = neo4j_date_from_string(date_start)
    date_end = neo4j_date_from_string(date_end)
    return date_start, date_end


def mail_title(result):
    if result is None:
        return ""
    return neo4j_date_to_string(result["date"]) + " | " + result["subject"]


def serialize_mail(mail):
    return {
        "subject": mail["Subject"],
        "date": neo4j_date_to_string(mail["Date"]),
        "from": mail["From"],
        "to": mail["To"],
    }


def serialize_employee(e):
    print(e["Email"])
    return {
        "email": str(e["Email"]),
        "name": e["Name"],
        "position": e["Position"],
        "sector": e["Sector"],
        "start_date": neo4j_str_date_to_string(e["StartDate"]),
    }


def create_graph(results):
    nodes, edges = list(), list()
    i = 0
    for record in results:
        nodes.append({"subject": mail_title(record), "label": "mail", "group": "M"})
        target = i
        i += 1
        for j, name in enumerate([record["author"]] + record["receivers"]):
            e = {"subject": name, "label": "person", "group": "P"}
            try:
                source = nodes.index(e)
            except ValueError:
                nodes += [e]
                source = i
                i += 1
            if j == 0:
                edges.append({"source": target, "target": source})
            else:
                edges.append(
                    {"source": source, "target": target, "type": "relationship type"}
                )
    return nodes, edges


def get_db():
    if not hasattr(g, "neo4j_db"):
        if neo4jVersion.startswith("4"):
            g.neo4j_db = driver.session(database=database)
        else:
            g.neo4j_db = driver.session()
    return g.neo4j_db


@app.teardown_appcontext
def close_db(error):
    if hasattr(g, "neo4j_db"):
        g.neo4j_db.close()


@app.route("/")
def get_index():
    return app.send_static_file("index.html")


CONDITION = (
    "m.Subject =~ $Subject "
    "and m.From =~ $from "
    "and m.To =~ $to  "
    "and datetime(m.Date) >= datetime( $start_date ) "
    "and m.Date <= datetime( $end_date ) "
    "and size(split(m.To, ',')) <= $to_limit "
)


def get_params():
    start_date = request.args["start_date"]
    end_date = request.args["end_date"]
    start_date, end_date = parse_dates(start_date, end_date)
    subject = request.args["subject"]
    _from = request.args["from"]
    to = request.args["to"]
    to_limit = request.args["limit"]
    to_limit = 1000 if to_limit == "" else int(to_limit)
    return start_date, end_date, subject, _from, to, to_limit


@app.route("/graph")
def get_graph():
    try:
        start_date, end_date, subject, _from, to, to_limit = get_params()
    except KeyError:
        return []
    else:
        db = get_db()
        results = db.write_transaction(
            lambda tx: list(
                tx.run(
                    "MATCH (b:Person)-[:SENT]->(m:Mail)-[:RECEIVED]->(a:Person) WHERE  "
                    + CONDITION
                    + "RETURN m.Date as date, "
                    "m.Subject as subject, "
                    "collect(a.Email) as receivers, "
                    "b.Email as author "
                    "LIMIT $limit",
                    {
                        "limit": 1000,
                        "Subject": regex(subject),
                        "start_date": start_date,
                        "end_date": end_date,
                        "from": regex(_from),
                        "to": regex(to),
                        "to_limit": to_limit,
                    },
                )
            )
        )
        nodes, edges = create_graph(results)
        return Response(
            dumps({"nodes": nodes, "links": edges}), mimetype="application/json"
        )


def regex(x):
    return "(?i).*" + x + ".*"


@app.route("/search")
def get_search():
    try:
        start_date, end_date, subject, _from, to, to_limit = get_params()
    except KeyError:
        return []
    else:
        db = get_db()
        results = db.read_transaction(
            lambda tx: list(
                tx.run(
                    "MATCH (m:Mail) " "WHERE " + CONDITION + "RETURN m as mail ",
                    {
                        "Subject": regex(subject),
                        "start_date": start_date,
                        "end_date": end_date,
                        "from": regex(_from),
                        "to": regex(to),
                        "to_limit": to_limit,
                    },
                )
            )
        )
        found = [serialize_mail(record["mail"]) for record in results]

        return Response(dumps({"found": found}), mimetype="application/json")


@app.route("/mail")
def get_employees():
    db = get_db()
    subject = request.args["subject"]
    date = neo4j_date_from_string(request.args["date"])
    _from = request.args["from"]
    to = request.args["to"]

    result = db.read_transaction(
        lambda tx: tx.run(
            "MATCH (a:Person)-[:SENT]->(m:Mail)-[:RECEIVED]->(b:Person) "
            "WHERE m.Subject = $subject "
            "and m.Date = datetime($date) "
            "and m.From = $from "
            "and m.To = $to "
            "RETURN m.Date as date, "
            "m.Subject as subject, "
            "a + COLLECT(distinct b) as employees "
            "LIMIT 1 ",
            {"subject": subject, "date": date, "from": _from, "to": to},
        ).single()
    )
    return Response(
        dumps(
            {
                "subject": mail_title(result),
                "employees": [serialize_employee(e) for e in result["employees"]]
                if result is not None
                else [],
            }
        ),
        mimetype="application/json",
    )


if __name__ == "__main__":
    logging.info("Running on port %d, database is at %s", port, url)
    app.run(port=port)
