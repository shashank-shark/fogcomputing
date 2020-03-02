import React from "react";

import {
    Position,
    Menu,
    MenuItem,
    MenuDivider,
    Popover,
    Icon, Button, Colors, Intent
} from '@blueprintjs/core';

import { useAuth0 } from "../../react-auth0-spa";
import {Link} from "react-router-dom";

const UserMenuItems = () => {

    const { logout } = useAuth0();

    return (
        <Menu>
            <Menu>
                <Link to="/profile" style={{ textDecoration: 'none', color: 'black'}}>
                    <MenuItem text="My Profile" icon="user" />
                </Link>
                <MenuItem text="Storage" icon="database" />
                <MenuItem text="Subscribers" icon="people" />
                <MenuDivider />
                <MenuItem text="Projects" icon="th" />
                <MenuItem text="Organization" icon="flow-review-branch" />
                <MenuDivider />
                <MenuItem text="Signout" icon="log-out" onClick={() => logout()} />
            </Menu>
        </Menu>
    );
};

const UserProfileMenu = (props) => {

    return (
        <Popover content={<UserMenuItems />} position={Position.BOTTOM_LEFT}>
            <Button style={styles.iconStyles}>
                <Icon style={styles.innstyle} icon="user" color={Colors.BLACK} />
            </Button>
        </Popover>
    );

};

const styles = {

    iconStyles : {
        margin: '2px'
    },
    innstyle: {
        margin: '2px'
    }
}

export default UserProfileMenu;