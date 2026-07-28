const div = (props, ...children) => createElement('div', props, children);
const span = (props, ...children) => createElement('span', props, children);
const ul = (props, ...children) => createElement('ul', props, children);
const li = (props, ...children) => createElement('li', props, children);
const p = (props, ...children) => createElement('p', props, children);
const a = (props, ...children) => createElement('a', props, children);
const pre = (props, ...children) => createElement('pre', props, children);
const h1 = (props, ...children) => createElement('h1', props, children);
const h2 = (props, ...children) => createElement('h2', props, children);
const table = (props, ...children) => createElement('table', props, children);
const tr = (props, ...children) => createElement('tr', props, children);
const th = (props, ...children) => createElement('th', props, children);
const td = (props, ...children) => createElement('td', props, children);

function createElement(name, props, children) {
    const element = document.createElement(name);
    if (props != null) {
        for (const [key, value] of Object.entries(props)) {
            element.setAttribute(key, value)
        }
    }
    const childrenArray = Array.isArray(children) ? children.flat() : [children];
    for (const child of childrenArray) {
        element.append(child)
    }

    return element;
}

const activeRender = new WeakMap();
async function render(parent, component, ...args) {
    const renderId = Symbol();
    activeRender.set(parent, renderId);

    const output = isFunction(component)
        ? component(...args)
        : component;

    if (isAsyncIterable(output)) {
        for await (const intermediate of output) {
            if (activeRender.get(parent) !== renderId) return; // New render into parent has started

            replaceChildren(parent, intermediate);
        }

        return;
    }

    const resolved = await output;
    if (activeRender.get(parent) !== renderId) return; // New render into parent has started
    replaceChildren(parent, resolved);
}


function replaceChildren(parent, children) {
    const c = Array.isArray(children)
        ? children.flat()
        : [children];

    parent.replaceChildren(...c);
}

function navigate(path) {
    history.pushState(null, "", path);
}

function router(render) {
    const run = async () => {
        try {
            await render();
        } catch (e) {
            console.error('Failed to render', e);
        }
    };

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', run, { once: true });
    } else {
        run()
    }

    document.addEventListener('popstate', run);

    document.addEventListener('click', (event) => {
        const link = event.target.closest('a')
        if (!link) return;

        if (
            event.button !== 0 ||
            event.ctrlKey ||
            event.metaKey ||
            event.shiftKey ||
            event.altKey
        ) {
            return;
        }

        event.preventDefault();
        navigate(link.pathname + link.search + link.hash);
        run();
    });
}

function isFunction(value) {
    return typeof value === 'function';
}
function isAsyncIterable(value) {
    return value != null && typeof value[Symbol.asyncIterator] === 'function';
}
