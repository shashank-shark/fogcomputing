import React from 'react';

import Navbar from "./components/NavBar";
import { useAuth0 } from "./react-auth0-spa";

import { BrowserRouter, Route, Switch } from "react-router-dom";

import Profile from "./components/Profile";

// import the screens
import HomeScreen from "./components/Screens/Home/HomeScreen";
import ServicesScreen from "./components/Screens/Services/ServicesScreen";
import AnalyticsScreen from "./components/Screens/Analytics/AnalyticsScreen";

import PrivateRoute from "./components/PrivateRoute";

import AppNavigationBar from "./components/Navbar/AppNavigationBar";
import NetworkSettings from "./components/Screens/Settings/NetworkSettings";

function App() {

  const { loading } = useAuth0();

  if (loading) {
    return <div>Loading...</div>
  }

  return (
      <div className="App">
          <BrowserRouter>

              <header>
                  <AppNavigationBar />
              </header>

              <Switch>

                  <Route path="/home" excat component={HomeScreen} />
                  <Route path="/services" excat component={ServicesScreen} />
                  <Route path="/analytics" excat component={AnalyticsScreen} />
                  <Route path="/networksettings" excat component={NetworkSettings} />

                  <PrivateRoute path="/profile" component={Profile} />

              </Switch>

          </BrowserRouter>
      </div>

  );
}

export default App;
