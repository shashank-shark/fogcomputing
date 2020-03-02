import React from 'react';
import ReactDOM from 'react-dom';

import * as serviceWorker from './serviceWorker';

import '@blueprintjs/core/lib/css/blueprint.css';
import '@blueprintjs/icons/lib/css/blueprint-icons.css';

import App from './App';
import { Auth0Provider} from "./react-auth0-spa";
import { authConfig} from "./auth.config";

const onRedirectCallback = appState => {
    window.history.replaceState(
        {},
        document.title,
        appState  && appState.targetUrl
        ? appState.targetUrl : window.location.pathname
    )
}

ReactDOM.render(
    <Auth0Provider
        domain={authConfig.domain} client_id={authConfig.clientId} redirect_uri={window.location.origin}
        onRedirectCallback={onRedirectCallback}
    >

        <App />
    </Auth0Provider>

   , document.getElementById('root'));

serviceWorker.unregister();
