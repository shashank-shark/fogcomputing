import React from "react";

import {
    Alignment,
    Button,
    Classes,
    H2, Intent,
    Navbar,
    NavbarDivider,
    NavbarGroup,
    NavbarHeading,
    Switch,

} from '@blueprintjs/core';

import { useAuth0 } from "../../react-auth0-spa";

import UserProfileMenu from "../Menu/UserProfileMenu";
import UserSettingsMenu from "../Menu/UserSettingsMenu";
import NotificationAndMessagesMenu from "../Menu/NotificationAndMessagesMenu";
import Profile from "../Profile";
import {Link} from "react-router-dom";

const AppNavigationBar = () => {

    // we get the states for auth
    const { isAuthenticated, loginWithRedirect, logout } = useAuth0();

    return (
      <div>
          <Navbar>

              <NavbarGroup align={Alignment.LEFT}>
                  <NavbarHeading style={styles.navbarHeading}><H2>Real Time Fog Computing</H2></NavbarHeading>
                  <NavbarDivider />
              </NavbarGroup>

              <NavbarGroup align={Alignment.RIGHT}>

                  {
                      isAuthenticated &&
                      <>
                          <Link to="/home">
                              <Button style={styles.navBarButton} icon="home" intent={Intent.PRIMARY}>
                                  <b>Home</b>
                              </Button>
                          </Link>

                          <Link to="/services">
                             <Button style={styles.navBarButton} icon="cloud" intent={Intent.PRIMARY}>
                                 <b>Services</b>
                             </Button>
                          </Link>

                          <Link to="/analytics">
                              <Button style={styles.navBarButton} icon="predictive-analysis" intent={Intent.PRIMARY}>
                                  <b>Alalytics</b>
                              </Button>
                          </Link>

                          <NavbarDivider />

                          <UserSettingsMenu />
                          <NotificationAndMessagesMenu />
                          <UserProfileMenu />
                      </>
                  }
              </NavbarGroup>

              <NavbarGroup align={Alignment.RIGHT}>
                  <NavbarDivider />
                  {
                      !isAuthenticated &&
                      <Button icon="log-in" intent={Intent.SUCCESS} onClick={() => loginWithRedirect({})}>
                          Signin
                      </Button>
                  }
              </NavbarGroup>
          </Navbar>
      </div>
    );
}

const styles = {

    navbarHeading : {
        backgroundColor: 'white',
        color: 'black',
        paddingTop: '6px'
    },

    navBarButton: {
        margin: '2px'
    }
}

export default AppNavigationBar;