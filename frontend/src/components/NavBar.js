import React from "react";

import { useAuth0} from "../react-auth0-spa";
import { Link } from "react-router-dom";

const Navbar = () => {

    const { isAuthenticated, loginWithRedirect, logout } = useAuth0();

    return (
      <div>
          {!isAuthenticated && (
              <button onClick={() => loginWithRedirect({})}>Log in</button>
          )}

          {isAuthenticated && <button onClick={() => logout()}>Log out</button> }

          {isAuthenticated && (
              <span>
                  <Link to="/">Home</Link>
                  <Link to="/profile">Profile</Link>
              </span>
          )}
      </div>
    );
}

export default Navbar;