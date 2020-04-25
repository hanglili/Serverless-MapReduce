from collections import namedtuple
from random import choice

from flask import Flask, jsonify

Quote = namedtuple("Quote", ("text", "author"))

quotes = [
    Quote("Talk is cheap. Show me the code.", "Linus Torvalds"),
    Quote("Programs must be written for people to read, and only incidentally for machines to execute.", "Harold Abelson"),
    Quote("Always code as if the guy who ends up maintaining your code will be a violent psychopath who knows where you live",
          "John Woods"),
    Quote("Give a man a program, frustrate him for a day. Teach a man to program, frustrate him for a lifetime.", "Muhammad Waseem"),
    Quote("Progress is possible only if we train ourselves to think about programs without thinking of them as pieces of executable code. ",
          "Edsger W. Dijkstra")
]

app = Flask(__name__)


@app.route("/", methods=["GET"])
def get_hello():
    return "Hello world"


@app.route("/quote", methods=["GET"])
def get_random_quote():
    return jsonify(choice(quotes)._asdict())
