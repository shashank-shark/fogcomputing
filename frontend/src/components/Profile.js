import React from "react";

import { useAuth0 } from "../react-auth0-spa";

const Profile = () => {

    const { loading, user} = useAuth0();

    if (loading || !user) {
        return (
            <div>Loading ...</div>
        );
    }

    return (
      <div>

          <img src={user.picture} />
          <h2>{user.name}</h2>
          <h2>{user.email}</h2>

          <code>{JSON.stringify(user, null, 2)}</code>
      </div>
    );

};

export default Profile;