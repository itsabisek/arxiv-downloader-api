from flask import Flask, render_template

app = Flask(__name__)

papers = [
    {
        'Name': "Paper 1",
        'Authors': "Author1, Author2",
        "Date Published": 'Date1'
    },
    {
        "Name": "Paper 2",
        'Authors': "Author3, Author4",
        "Date Published": 'Date2'
    }
]


@app.route('/')
@app.route("/home")
def home():
    return render_template('home.html', papers=papers)


@app.route("/about")
def about():
    return render_template('about.html', title="About")


if __name__ == "__main__":
    app.run(debug=True)
