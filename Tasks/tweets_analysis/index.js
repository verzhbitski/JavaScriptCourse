'use strict';

const LineByLineReader = require('line-by-line'),
    lr = new LineByLineReader('./input/dataSet.csv');

const data = [];

lr.on('error', function (err) {
    // 'err' contains error object
});

let header;
let isHeader = true;

let multiline = '';

let wordsFreq = {};
let tweetRts = {};
let tweetAuthors = {};
let producingCountires = {};
let consumingCountries ={};
let authors = {};

function processText(tweetText) {
    let words = tweetText.match(/\b[a-z]+\b/gi);

    words.forEach(function (word) {
        if (wordsFreq[word] === undefined) {
            wordsFreq[word] = 1;
        } else {
            wordsFreq[word] += 1;
        }
    });
}

function processCountry(tweetCountry, tweetText) {
    if (tweetCountry.length === 0) {
        return;
    }

    if (tweetText.substr(0, 2) === 'RT') {
        if (consumingCountries[tweetCountry] === undefined) {
            consumingCountries[tweetCountry] = 1;
        } else {
            consumingCountries[tweetCountry] += 1;
        }
    } else {
        if (producingCountires[tweetCountry] === undefined) {
            producingCountires[tweetCountry] = 1;
        } else {
            producingCountires[tweetCountry] += 1;
        }
    }
}

function processAuthor(tweetAuthor, tweetRts) {
    let rt = parseInt(tweetRts);

    if (isNaN(rt)) {
        return;
    }

    if (authors[tweetAuthor] === undefined) {
        authors[tweetAuthor] = rt;
    } else {
        authors[tweetAuthor] += rt;
    }
}

function processTweet(tweet) {
    let splitted = tweet.split(';');

    if (isNaN(parseInt(splitted[header.indexOf('RTs')]))) {
        let rts = parseInt(splitted[header.indexOf('RTs')]);
        if (tweetRts[splitted[header.indexOf('Tweet content')]] === undefined) {
            tweetRts[splitted[header.indexOf('Tweet content')]] = rts;
            tweetAuthors[splitted[header.indexOf('Tweet content')]] = splitted[header.indexOf('Nickname')];
        }
    }

    processText(splitted[header.indexOf('Tweet content')]);
    processCountry(splitted[header.indexOf('Country')], splitted[header.indexOf('Tweet content')]);
    processAuthor(splitted[header.indexOf('Nickname')], splitted[header.indexOf('RTs')]);
}


lr.on('line', function (line) {

    if (isHeader) {
        isHeader = false;
        header = line.split(';');
        return;
    }

    // pause emitting of lines...

    lr.pause();

    // ...do your asynchronous line processing..

    let fullTweet;
    let isFullTweet = false;

    setTimeout(function () {
        // ...and continue emitting lines.

        if (line.split(';').length === 19) {
            fullTweet = line;
            isFullTweet = true;
            data.push(line);
        } else {
            multiline += ' ' + line;
            if (multiline.split(';').length === 19) {
                fullTweet = multiline;
                isFullTweet = true;
                data.push(multiline);
                multiline = '';
            }
        }
        lr.resume();
    }, 100);

    if (isFullTweet) {
        processTweet(fullTweet);
    }

});

lr.on('end', function () {
    // All lines are read, file is closed now.
    main();
});

function getFreqWords() {
    return new Promise((resolve, reject) => {
        let items = Object.keys(wordsFreq).map(function(key) {
            return [key, wordsFreq[key]];
        });

        items.sort(function(first, second) {
            return second[1] - first[1];
        });
        resolve(items.slice(0, 10));
    });
}

function getPopularAuthors() {
    return new Promise((resolve, reject) => {
        let items = Object.keys(authors).map(function(key) {
            return [key, authors[key]];
        });

        items.sort(function(first, second) {
            return second[1] - first[1];
        });
        resolve(items.slice(0, 10));
    });
}

function getPopularTweets() {
    return new Promise((resolve, reject) => {
        let items = Object.keys(tweetRts).map(function(key) {
            return [key, tweetRts[key]];
        });

        items.sort(function(first, second) {
            return second[1] - first[1];
        });
        resolve(items.slice(0, 10));
    });
}

function getProducingCountries() {
    return new Promise((resolve, reject) => {
        let items = Object.keys(producingCountires).map(function(key) {
            return [key, producingCountires[key]];
        });

        items.sort(function(first, second) {
            return second[1] - first[1];
        });
        resolve(items);
    });
}

function getConsumingCountries() {
    return new Promise((resolve, reject) => {
        let items = Object.keys(consumingCountries).map(function(key) {
            return [key, consumingCountries[key]];
        });

        items.sort(function(first, second) {
            return second[1] - first[1];
        });
        resolve(items);
    });
}

function output(result) {
    console.log('1 ...');
    result[0].forEach(function (word) {
        console.log(word[0] + ' : ' + word[1]);
    });

    console.log('\n\n');
    console.log('2 ...');
    result[1].forEach(function (tweet) {
        console.log(tweet[0] + ' : ' + tweetAuthors[tweet[0]] + ' : ' + tweet[1]);
    });

    console.log('\n\n');
    console.log('3 ...');
    result[2].forEach(function (author) {
        console.log(author[0] + ' : ' + author[1]);
    });

    console.log('\n\n');
    console.log('4 ...');
    console.log('Producing countries');
    console.log('\n');
    result[3].forEach(function (country) {
        console.log(country[0] + ' : ' + country[1]);
    });

    console.log('\n\n');
    console.log('Consuming countries');
    console.log('\n');
    result[4].forEach(function (country) {
        console.log(country[0] + ' : ' + country[1]);
    });
}

function main () {
    let promises = [];

    promises.push(getFreqWords());
    promises.push(getPopularTweets());
    promises.push(getPopularAuthors());
    promises.push(getProducingCountries());
    promises.push(getConsumingCountries());
    Promise.all(promises).then((result) => {output(result);});
}