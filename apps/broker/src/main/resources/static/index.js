const dummyTopics = {
    'serg': {
        total: 100_000,
        consumers: {
            m22: 50_000,
            digibok: 30_000,
        }
    },
    'freg': {
        total: 200_000,
        consumers: {
            m22: 10_000,
            digibok: 3_000,
        }
    },
}

const mainView = document.querySelector('.main');
const asideView = document.querySelector('.aside');

router(async () => {
    const urlParams = new URLSearchParams(window.location.search);
    const currentTopic = urlParams.get('topic');

    await Promise.all([
        render(asideView, Menu, Object.keys(dummyTopics)),
        currentTopic == null
            ? render(mainView, NoTopics)
            : render(mainView, TopicView, currentTopic)
    ]);
});

async function NoTopics() {
    return p(null, 'Select topic to inrospect from the menu');
}

async function* TopicView(currentTopic) {
    yield div(null,
        h2({ 'class': 'blokk-l' }, `Topic: ${currentTopic}`),
        div(null, 'Loading...')
    );

    const data = await fetchTopicData(currentTopic);

    yield div(null,
        h2({ 'class': 'blokk-l' }, `Topic: ${currentTopic}`),
        p({ 'class': 'blokk-s' }, `Antall meldinger: ${data.total}`),
        ConsumerTable(data.consumers),
    );
}

function Menu(topics) {
    return [
        a({ href: '?' }, 'Topics'),
        ul(null, topics.map(TopicLink))
    ]
}

function TopicLink(topic) {
    return (
        li(null,
            a({ href: `?topic=${topic}`}, topic)
        )
    )
}

function ConsumerTable(consumers) {
    const sortedConsumers = Object.entries(consumers)
        .toSorted(([aKey, aValue], [bKey, bValue]) => {
            return aValue - bValue;
        });

    return table(null,
        tr(null,
            th(null, 'Consumer group'),
            th(null, 'Offset'),
        ),
        sortedConsumers.map(([name, offset]) =>
            tr(null,
                td(null, name),
                td(null, offset),
            )
        )
    );
}

async function fetchTopicData(topic) {
    if (window.location.origin.includes('localhost')) {
        return new Promise((resolve, reject) => {
            setTimeout(() => {
                const topicData = dummyTopics[topic];
                if (topicData) resolve(topicData)
                else reject()
            }, 250);
        });
    } else {
        return fetch(`/topics/${topic}/introspect`)
            .then(r => r.json())
    }
}